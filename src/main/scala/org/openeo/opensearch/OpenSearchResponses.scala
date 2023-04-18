package org.openeo.opensearch

import _root_.io.circe.parser.decode
import cats.syntax.either._
import cats.syntax.show._
import geotrellis.proj4.util.UTM
import geotrellis.proj4.{CRS, LatLng}
import io.circe.generic.auto._
import io.circe.{Decoder, HCursor, Json, JsonObject}
import geotrellis.vector._
import org.slf4j.LoggerFactory
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}

import java.io.{FileInputStream, FileNotFoundException, InputStream}
import java.lang.System.getenv
import java.net.URI
import java.nio.file.Paths
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.util.regex.Pattern
import javax.net.ssl.HttpsURLConnection
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}
import scala.xml.{Node, XML}

object OpenSearchResponses {

  private val logger = LoggerFactory.getLogger(classOf[OpenSearchClient])
  implicit val decodeUrl: Decoder[URI] = Decoder.decodeString.map(s => URI.create(s.trim))
  implicit val decodeDate: Decoder[ZonedDateTime] = Decoder.decodeString.map(s => ZonedDateTime.parse(s.split('/')(0)))

  case class Link(href: URI, title: Option[String])

  /**
   * To store some simple properties that come out of the "properties" JSON node.
   * Properties that need some processing are better parsed in the apply functions.
   */
  case class GeneralProperties(published: Option[ZonedDateTime], orbitNumber: Option[Int],
                               organisationName: Option[String], instrument: Option[String]) {
    def this() = this(None, None, None, None)
  }

  case class Feature(id: String, bbox: Extent, nominalDate: ZonedDateTime, links: Array[Link], resolution: Option[Double],
                     tileID: Option[String] = None, geometry: Option[Geometry] = None, var crs: Option[CRS] = None,
                     generalProperties: GeneralProperties = new GeneralProperties(), var rasterExtent: Option[Extent] = None
                     ){
    crs = crs.orElse{ for {
      id <- tileID if id.matches("[0-9]{2}[A-Z]{3}")
      utmEpsgStart = if (id.charAt(2) >= 'N') "326" else "327"
    } yield CRS.fromEpsgCode((utmEpsgStart + id.substring(0, 2)).toInt) }
    if(tileID.isDefined && crs.isDefined && crs.get.proj4jCrs.getProjection.getName == "utm") {
      val bboxUTM = MGRS.mgrsToSentinel2Extent(tileID.get)
      rasterExtent = Some(bboxUTM)

    }else if(crs.contains(LatLng)){
      rasterExtent = Some(bbox)
    }

  }

  private def isDuplicate(d1: Option[ZonedDateTime], d2: Option[ZonedDateTime]): Boolean = {
    if (d1.isDefined != d2.isDefined) return false
    if (d1.isDefined && d2.isDefined
      && ChronoUnit.SECONDS.between(d1.get, d2.get) > 30) return false
    true
  }

  private def isDuplicate(f1: Feature, f2: Feature): Boolean = {
    if (ChronoUnit.SECONDS.between(f1.nominalDate, f2.nominalDate) > 30) return false

    if (!isDuplicate(f1.generalProperties.published, f2.generalProperties.published)) return false

    // If orbitNumber or organisationName is None it works out too
    if (f1.generalProperties.orbitNumber != f2.generalProperties.orbitNumber) return false
    if (f1.generalProperties.instrument != f2.generalProperties.instrument) return false
    if (f1.generalProperties.organisationName != f2.generalProperties.organisationName) return false
    if (f1.resolution != f2.resolution) return false

    if (f1.geometry.isDefined && !f1.geometry.get.equalsExact(f2.geometry.get, 0.0001)) return false
    true
  }

  /**
   * Should be under O(n*n)
   */
  def dedupFeatures(features: Array[Feature]): Array[Feature] = {
    val featuresSorted = features.sortBy(_.nominalDate)

    val dupClusters = scala.collection.mutable.Map[Feature, ListBuffer[Feature]]()
    // Only check for dups in a cluster of Features with the same startDate for performance.
    var dateClusterStart = 0
    for (i <- featuresSorted.indices) {

      if (i != dateClusterStart && ChronoUnit.SECONDS.between(featuresSorted(dateClusterStart).nominalDate,
        featuresSorted(i).nominalDate) > 30) {
        // time gap since previous Feature, so make a cut for better performance
        dateClusterStart = i
      }

      var foundDupToAttachTo = false
      breakable {
        for (j <- dateClusterStart until i) {
          if (isDuplicate(featuresSorted(i), featuresSorted(j))) {
            dupClusters(featuresSorted(j)) += featuresSorted(i)
            foundDupToAttachTo = true
            break
          }
        }
      }
      if (!foundDupToAttachTo) {
        dupClusters += (featuresSorted(i) -> ListBuffer(featuresSorted(i)))
      }
    }

    val featuresGroupedFiltered = dupClusters.map({ case (key, features) =>
      val selectedElement = features.maxBy(_.generalProperties.published)
      val toBeRemoved = features.filter(_ != selectedElement)
      val toLog = if (toBeRemoved.nonEmpty)
        ("Removing duplicated feature(s): " + toBeRemoved.map("'" + _.id + "'").mkString(", ")
          + ". Keeping the Latest published one: '" + selectedElement.id) + "'"
      else
        ""
      (key, selectedElement, toLog)
    })

    val msg = featuresGroupedFiltered
      .map({ case (_, _, toLog) => toLog })
      .filter(_ != "")
      .mkString("\n")
    if (msg.nonEmpty) logger.info(msg)
    val featuresFiltered = featuresGroupedFiltered.map({ case (_, selectedElement, _) => selectedElement }).toSet

    // Make sure the order is as it was before the dedup
    features.flatMap(f => if (featuresFiltered.contains(f)) List(f) else List())
  }

  case class FeatureCollection(itemsPerPage: Int, features: Array[Feature])

  object FeatureCollection {
    /**
     * Should only dedup when getting Products. Not when getting collections
     */
    def parse(json: String, isUTM: Boolean = false, dedup: Boolean = false): FeatureCollection = {
      implicit val decodeFeature: Decoder[Feature] = new Decoder[Feature] {
        override def apply(c: HCursor): Decoder.Result[Feature] = {
          for {
            id <- c.downField("id").as[String]
            bbox <- c.downField("bbox").as[Array[Double]]
            nominalDate <- c.downField("properties").downField("date").as[ZonedDateTime]
            links <- c.downField("properties").downField("links").as[Map[String, Array[Link]]]
            resolution = c.downField("properties").downField("productInformation").downField("resolution").downArray.first.as[Double].toOption
            maybeCRS = c.downField("properties").downField("productInformation").downField("referenceSystemIdentifier").as[String].toOption
            tileId = c.downField("properties").downField("acquisitionInformation").as[List[JsonObject]].toOption.flatMap(params => params.find(n => n.contains("acquisitionParameters")).flatMap(_ ("acquisitionParameters")).map(_ \\ "tileId")).flatMap(_.headOption.flatMap(_.asString))
            properties <- c.downField("properties").as[GeneralProperties]
          } yield {
            val Array(xMin, yMin, xMax, yMax) = bbox
            val extent = Extent(xMin, yMin, xMax, yMax)
            val geometry = c.downField("geometry").as[Geometry].toOption
            val prefix = "https://www.opengis.net/def/crs/EPSG/0/"
            val crs =
            if(isUTM && tileId.isEmpty){
              //this is ugly, but not having a crs is worse, should be fixed in the catalogs
              Some(UTM.getZoneCrs(extent.center.x,extent.center.y))
            }else {
              if (maybeCRS.isDefined && maybeCRS.get.startsWith(prefix)) {
                try {
                  val epsg = maybeCRS.get.substring(prefix.length)
                  Some(CRS.fromEpsgCode(epsg.toInt))

                }catch {
                  case e: Exception => logger.debug(s"Invalid projection while parsing ${id}, error: ${e.getMessage}")
                    None
                }
              } else {
                None
              }
            }

            var res = resolution
            if (res.isEmpty) {
              // needed for oscars:
              res = c.downField("properties").downField("additionalAttributes").downField("resolution").as[Double].toOption
            }

            Feature(id, extent, nominalDate, links.values.flatten.toArray, res,
              tileId, geometry = geometry, crs = crs, generalProperties=properties)
          }
        }
      }

      implicit val decodeFeatureCollection: Decoder[FeatureCollection] = new Decoder[FeatureCollection] {
        override def apply(c: HCursor): Decoder.Result[FeatureCollection] = {
          for {
            itemsPerPage <- c.downField("itemsPerPage").as[Int]
            features <- c.downField("features").as[Array[Feature]]
          } yield {
            val featuresFiltered = if (dedup) dedupFeatures(features) else features
            FeatureCollection(itemsPerPage, featuresFiltered)
          }
        }
      }

      decode[FeatureCollection](json)
        .valueOr(e => throw new IllegalArgumentException(s"${e.show} while parsing '$json'", e))
    }
  }

  object STACFeatureCollection {
    def parse(json: String, toS3URL: Boolean = true, dedup: Boolean = false): FeatureCollection = {
      implicit val decodeFeature: Decoder[Feature] = new Decoder[Feature] {
        override def apply(c: HCursor): Decoder.Result[Feature] = {
          for {
            id <- c.downField("id").as[String]
            bbox <- c.downField("bbox").as[Array[Double]]
            nominalDate <- c.downField("properties").downField("datetime").as[ZonedDateTime]
            links <- c.downField("assets").as[Map[String, Link]]
            resolution = c.downField("properties").downField("gsd").as[Double].toOption
            properties <- c.downField("properties").as[GeneralProperties]
          } yield {
            val Array(xMin, yMin, xMax, yMax) = bbox
            val extent = Extent(xMin, yMin, xMax, yMax)
            val geometry = c.downField("geometry").as[Geometry].toOption

            val harmonizedLinks = links.map { t =>
              val href = t._2.href
              if(toS3URL){
                val bucket = href.getHost.split('.')(0)
                val s3href = URI.create("s3://" + bucket + href.getPath)
                Link(s3href, Some(t._1))
              }
              else{
                Link(href, Some(t._1)) }
            }
            Feature(id, extent, nominalDate, harmonizedLinks.toArray, resolution, None, geometry = geometry,
              generalProperties=properties)
          }
        }
      }

      implicit val decodeFeatureCollection: Decoder[FeatureCollection] = new Decoder[FeatureCollection] {
        override def apply(c: HCursor): Decoder.Result[FeatureCollection] = {
          for {
            itemsPerPage <- c.downField("numberReturned").as[Int]
            features <- c.downField("features").as[Array[Feature]]
          } yield {
            val featuresFiltered = if (dedup) dedupFeatures(features) else features
            FeatureCollection(itemsPerPage, featuresFiltered)
          }
        }
      }

      decode[FeatureCollection](json)
        .valueOr(e => throw new IllegalArgumentException(s"${e.show} while parsing '$json'", e))
    }
  }

  object CreoFeatureCollection {

    private val TILE_PATTERN = Pattern.compile("_T([0-9]{2}[A-Z]{3})_")
    private val s3Endpoint = System.getenv().getOrDefault("AWS_S3_ENDPOINT","")//https://s3.cloudferro.com
    private val useHTTPS = System.getenv().getOrDefault("AWS_HTTPS","YES")//https://s3.cloudferro.com

    private val creoClient = {
      if(s3Endpoint!="") {
        val uri =
        if(s3Endpoint.startsWith("http")) {
          new URI( s3Endpoint )
        }else if(useHTTPS == "NO"){
          new URI( "http://" + s3Endpoint )
        }else{
          new URI( "https://" + s3Endpoint )
        }
        Some(S3Client.builder.endpointOverride(uri).region(Region.of("RegionOne"))
          .serviceConfiguration(S3Configuration.builder.pathStyleAccessEnabled(true).build).build())
      }else{
        Option.empty
      }
    }

    private def getAwsDirect() = {
      "TRUE".equals(getenv("AWS_DIRECT"))
    }

    def loadMetadata(path:String, metadatafile:String):InputStream = {
      var gdalPrefix = ""
      val inputStream = if (path.startsWith("https://")) {
        gdalPrefix = "/vsicurl"

        val uri = new URI(path)
        return uri.resolve(s"${uri.getPath}/${metadatafile}").toURL
          .openConnection.asInstanceOf[HttpsURLConnection]
          .getInputStream
      } else {
        gdalPrefix = if (getAwsDirect()) "/vsis3" else ""

        if (path.startsWith("/eodata")) {

          //reading from /eodata is extremely slow
          if (creoClient.isDefined) {
            return creoClient.get.getObject(GetObjectRequest.builder().bucket("EODATA").key(s"${path.toString.replace("/eodata/", "")}/${metadatafile}").build())
          } else {
            val url = path.replace("/eodata", "https://finder.creodias.eu/files")
            val uri = new URI(url)
            try {
              return uri.resolve(s"${uri.getPath}/$metadatafile").toURL
                .openConnection.getInputStream
            } catch {
              case e: FileNotFoundException => return null
            }
          }

        } else {
          return new FileInputStream(Paths.get(path, metadatafile).toFile)
        }
      }
      return inputStream
    }

    private def getGDALPrefix(path:String) = {
      var gdalPrefix = ""
      if (path.startsWith("https://")) {
        gdalPrefix = "/vsicurl"
      } else if (getAwsDirect()) {
        gdalPrefix = "/vsis3"
      }
      gdalPrefix
    }

    private def getFilePathsFromManifest(path: String): Seq[Link] = {

      val gdalPrefix: String = getGDALPrefix(path)
      val inputStream: InputStream = loadMetadata(path,"manifest.safe")
      if(inputStream == null) {
        return Seq.empty[Link]
      }
      val xml = XML.load(inputStream)

      (xml \\ "dataObject" )
        .map((dataObject: Node) =>{
          val title = dataObject \\ "@ID"
          val fileLocation = dataObject \\ "fileLocation" \\ "@href"
          Link(URI.create(s"$gdalPrefix${if (path.startsWith("/")) "" else "/"}$path" + s"/${URI.create(fileLocation.toString).normalize().toString}"), Some(title.toString))
        })
    }

    /**
     * Creo catalogs do not point to the actual file, so we need to custom lookups
     * @param path
     * @return
     */
    private def getDEMPathFromInspire(path: String): Seq[Link] = {
      val gdalPrefix: String = getGDALPrefix(path)
      val inputStream: InputStream = loadMetadata(path, "INSPIRE.xml")
      if(inputStream == null) {
        return Seq.empty[Link]
      }
      val xml = XML.load(inputStream)

      (xml \\ "CI_Citation" \ "identifier" \ "RS_Identifier" \ "code"  )
        .map((dataObject: Node) =>{
          val title = (dataObject \ "CharacterString").text
          val demPath = title.split(':')(2)
          val fileLocation = s"${path}/${demPath}/DEM/${demPath}_DEM.tif"
          Link(URI.create(s"$gdalPrefix${if (path.startsWith("/")) "" else "/"}" + s"${URI.create(fileLocation.toString).normalize().toString}"), Some("DEM"))
        })
    }

    private def ensureValidGeometry(geometry: Json): Json = {
      // TODO: This is required because the old Creodias API can return incorrect MultiPolygon geometries.
      // This can be removed once the API is fixed.
      val c = geometry.hcursor
      val geometryType = c.downField("type").as[String].getOrElse("")
      if (geometryType == "MultiPolygon") {
        val coordinates = c.downField("coordinates").as[Json].getOrElse(Json.Null)
        var depth = 0
        var current = coordinates
        while (current.isArray) {
          depth += 1
          current = current.asArray.get.head
        }
        if (depth == 3) {
          // MultiPolygons should always have a depth of 4.
          // val newCoordinates = Json.arr(coordinates)
          return Json.obj(
            ("type", Json.fromString("Polygon")),
            ("coordinates", coordinates)
          )
        }
      }
      geometry
    }

    def parse(json: String, dedup: Boolean = false): FeatureCollection = {
      implicit val decodeFeature: Decoder[Feature] = new Decoder[Feature] {
        override def apply(c: HCursor): Decoder.Result[Feature] = {
          for {
            id <- c.downField("properties").downField("productIdentifier").as[String]
            geometry <- c.downField("geometry").as[Json]
            nominalDate <- c.downField("properties").downField("startDate").as[ZonedDateTime]
            links <- c.downField("properties").downField("links").as[Array[Link]]
            resolution = c.downField("properties").downField("resolution").as[Double].toOption
            properties <- c.downField("properties").as[GeneralProperties]
          } yield {
            val theGeometry = ensureValidGeometry(geometry).toString().parseGeoJson[Geometry]
            val extent = theGeometry.extent
            val tileIDMatcher = TILE_PATTERN.matcher(id)
            val tileID =
            if(tileIDMatcher.find()){
              Some(tileIDMatcher.group(1))
            }else{
              Option.empty
            }

            if(id.contains("COP-DEM_GLO-30-DGED")){
              val all_links = getDEMPathFromInspire(id)
              Feature(id, extent, nominalDate, all_links.toArray, resolution,tileID,Option(theGeometry), generalProperties=properties)
            }else{
              val all_links = getFilePathsFromManifest(id)
              Feature(id, extent, nominalDate, all_links.toArray, resolution,tileID,Option(theGeometry), generalProperties=properties)
            }
          }
        }
      }



      implicit val decodeFeatureCollection: Decoder[FeatureCollection] = new Decoder[FeatureCollection] {
        override def apply(c: HCursor): Decoder.Result[FeatureCollection] = {
          for {
            itemsPerPage <- c.downField("properties").downField("itemsPerPage").as[Int]
            features <- c.downField("features").as[Array[Feature]]
          } yield {
            val featuresFiltered = if (dedup) dedupFeatures(features) else features
            FeatureCollection(itemsPerPage, featuresFiltered)
          }
        }
      }

      decode[FeatureCollection](json)
        .valueOr(e => throw new IllegalArgumentException(s"${e.show} while parsing '$json'", e))
    }
  }

  case class STACCollection(id: String)
  case class STACCollections(collections: Array[STACCollection])

  object STACCollections {
    def parse(json: String): STACCollections = {
      decode[STACCollections](json)
        .valueOr(e => throw new IllegalArgumentException(s"${e.show} while parsing '$json'", e))
    }
  }

  case class CreoCollection(name: String)
  case class CreoCollections(collections: Array[CreoCollection])

  object CreoCollections {
    def parse(json: String): CreoCollections = {
      decode[CreoCollections](json)
        .valueOr(e => throw new IllegalArgumentException(s"${e.show} while parsing '$json'", e))
    }
  }
}
