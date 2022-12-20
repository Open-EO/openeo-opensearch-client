package org.openeo.opensearch

import _root_.io.circe.parser.decode
import cats.syntax.either._
import cats.syntax.show._
import geotrellis.proj4.CRS
import geotrellis.proj4.util.UTM
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
import java.util.regex.Pattern
import javax.net.ssl.HttpsURLConnection
import scala.xml.{Node, XML}

object OpenSearchResponses {

  private val logger = LoggerFactory.getLogger(classOf[OpenSearchClient])
  implicit val decodeUrl: Decoder[URI] = Decoder.decodeString.map(URI.create)
  implicit val decodeDate: Decoder[ZonedDateTime] = Decoder.decodeString.map(s => ZonedDateTime.parse(s.split('/')(0)))

  case class Link(href: URI, title: Option[String])

  case class Feature(id: String, bbox: Extent, nominalDate: ZonedDateTime, links: Array[Link], resolution: Option[Double],
                     tileID: Option[String] = None, geometry: Option[Geometry] = None, var crs: Option[CRS] = None,
                     publishedDate: Option[ZonedDateTime] = None){
    crs = crs.orElse{ for {
      id <- tileID if id.matches("[0-9]{2}[A-Z]{3}")
      utmEpsgStart = if (id.charAt(2) >= 'N') "326" else "327"
    } yield CRS.fromEpsgCode((utmEpsgStart + id.substring(0, 2)).toInt) }
  }

  case class FeatureCollection(itemsPerPage: Int, features: Array[Feature])

  object FeatureCollection {
    def parse(json: String, isUTM: Boolean = false): FeatureCollection = {
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
            Feature(id, extent, nominalDate, links.values.flatten.toArray, resolution,tileId,geometry=geometry,crs = crs)
          }
        }
      }

      decode[FeatureCollection](json)
        .valueOr(e => throw new IllegalArgumentException(s"${e.show} while parsing '$json'", e))
    }
  }

  object STACFeatureCollection {
    def parse(json: String, toS3URL:Boolean=true): FeatureCollection = {
      implicit val decodeFeature: Decoder[Feature] = new Decoder[Feature] {
        override def apply(c: HCursor): Decoder.Result[Feature] = {
          for {
            id <- c.downField("id").as[String]
            bbox <- c.downField("bbox").as[Array[Double]]
            nominalDate <- c.downField("properties").downField("datetime").as[ZonedDateTime]
            links <- c.downField("assets").as[Map[String, Link]]
            resolution = c.downField("properties").downField("gsd").as[Double].toOption
          } yield {
            val Array(xMin, yMin, xMax, yMax) = bbox
            val extent = Extent(xMin, yMin, xMax, yMax)
            val geometry = c.downField("geometry").as[Geometry].toOption

            val harmonizedLinks = links.map { t =>
              val href = t._2.href
              if(toS3URL){
                val bucket = href.getHost.split('.')(0)
                val s3href = URI.create("s3://" + bucket +href.getPath)
                Link(s3href, Some(t._1))
              }
              else{
                Link(href, Some(t._1)) }
            }
            Feature(id, extent, nominalDate, harmonizedLinks.toArray, resolution,None,geometry=geometry)
          }
        }
      }

      implicit val decodeFeatureCollection: Decoder[FeatureCollection] = new Decoder[FeatureCollection] {
        override def apply(c: HCursor): Decoder.Result[FeatureCollection] = {
          for {
            itemsPerPage <- c.downField("numberReturned").as[Int]
            features <- c.downField("features").as[Array[Feature]]
          } yield {
            FeatureCollection(itemsPerPage, features)
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
          Link(URI.create(s"$gdalPrefix${if (path.startsWith("/")) "" else "/"}$path" + s"/${Paths.get(fileLocation.toString).normalize().toString}"),Some(title.toString))
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
          Link(URI.create(s"$gdalPrefix${if (path.startsWith("/")) "" else "/"}" + s"${Paths.get(fileLocation.toString).normalize().toString}"),Some("DEM"))
        })
    }

    def parse(json: String): FeatureCollection = {
      implicit val decodeFeature: Decoder[Feature] = new Decoder[Feature] {
        override def apply(c: HCursor): Decoder.Result[Feature] = {
          for {
            id <- c.downField("properties").downField("productIdentifier").as[String]
            geometry <- c.downField("geometry").as[Json]
            nominalDate <- c.downField("properties").downField("startDate").as[ZonedDateTime]
            links <- c.downField("properties").downField("links").as[Array[Link]]
            resolution = c.downField("properties").downField("resolution").as[Double].toOption
            publishedDate = c.downField("properties").downField("published").as[ZonedDateTime].toOption
          } yield {

            val theGeometry = geometry.toString().parseGeoJson[Geometry]
            val extent = theGeometry.extent
            val tileIDMatcher = TILE_PATTERN.matcher(id)
            val tileID =
            if(tileIDMatcher.find()){
              Some(tileIDMatcher.group(1))
            }else{
              Option.empty
            }

            if(id.endsWith(".SAFE")){
              val all_links = getFilePathsFromManifest(id)
              Feature(id, extent, nominalDate, all_links.toArray, resolution,tileID,Option(theGeometry), None, publishedDate)
            }else if(id.contains("COP-DEM_GLO-30-DGED")){
              val all_links = getDEMPathFromInspire(id)
              Feature(id, extent, nominalDate, all_links.toArray, resolution,tileID,Option(theGeometry), None, publishedDate)
            }else{
              Feature(id, extent, nominalDate, links, resolution,tileID,Option(theGeometry), None, publishedDate)
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
            val featuresDeduped = features
              .groupBy(x => (x.nominalDate, x.geometry)) // Simple check to assume equality
              .map(tuple => tuple._2.maxBy(_.publishedDate))
              .toArray
            if (featuresDeduped.length < features.length) {
              logger.debug(s"A duplicate was removed from the results. Most recent published was kept")
            }
            FeatureCollection(itemsPerPage, featuresDeduped)
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
