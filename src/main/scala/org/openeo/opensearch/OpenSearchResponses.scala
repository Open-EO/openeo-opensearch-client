package org.openeo.opensearch

import _root_.io.circe.parser.decode
import cats.syntax.either._
import cats.syntax.show._
import geotrellis.proj4.util.UTM
import geotrellis.proj4.{CRS, LatLng}
import io.circe.generic.auto._
import io.circe.{Decoder, HCursor, Json, JsonObject}
import geotrellis.vector._
import org.locationtech.jts.simplify.TopologyPreservingSimplifier
import org.slf4j.LoggerFactory
import software.amazon.awssdk.awscore.retry.conditions.RetryOnErrorCodeCondition
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy
import software.amazon.awssdk.core.retry.conditions.{OrRetryCondition, RetryCondition, RetryOnExceptionsCondition, RetryOnStatusCodeCondition}
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, NoSuchKeyException}
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}

import java.io.{FileInputStream, FileNotFoundException, InputStream}
import java.lang.System.getenv
import java.net.{SocketTimeoutException, URI}
import java.nio.file.Paths
import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}
import java.util.regex.Pattern
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Using
import scala.util.control.Breaks.{break, breakable}
import scala.util.matching.Regex
import scala.xml.{Node, XML}

object OpenSearchResponses {

  private val logger = LoggerFactory.getLogger(classOf[OpenSearchClient])
  implicit val decodeUrl: Decoder[URI] = Decoder.decodeString.map(s => URI.create(s.trim))
  implicit val decodeDate: Decoder[ZonedDateTime] = Decoder.decodeString.map(s => ZonedDateTime.parse(s.split('/')(0)))


  /**
   * Method to align various naming schemes used by Sentinel-2 into the same format
   *
   * It's quite messy.
   *
   * IMG_DATA_60m_Band9_Tile1_Data      2019 99.99
   * IMG_DATA_Band_B09_60m_Tile1_Data   2021
   * IMG_DATA_Band_60m_2_Tile1_Data     2016 T26SPG_20160119T123042_B05.jp2
   *
   * @param title
   * @return
   */
  def sentinel2Reformat(title: String, href:String): String = {
    if (title == "S2_Level-2A_Tile1_Data" && href.contains("L2A")) {
      // logic for processingBaseline 2.07 and 99.99
      return "S2_Level-2A_Tile1_Metadata"
    }
    val patternAuxData: Regex = """(...)_DATA_(\d{2}m)_Tile1_Data""".r

    val patternHref:Regex = """.*_(B.._\d{2}m).jp2""".r

    href match {
      case patternHref(resBand) => return f"IMG_DATA_Band_${resBand}_Tile1_Data"
      case _ => None
    }

    title match {
      case patternAuxData(name,resolution) =>
        return f"IMG_DATA_Band_${name}_${resolution}_Tile1_Data"

      case _ =>
        title
    }
  }


  case class Link(href: URI, title: Option[String], pixelValueOffset: Option[Double] = Some(0),
                  bandNames: Option[Seq[String]] = None)

  /**
   * To store some simple properties that come out of the "properties" JSON node.
   * Properties that need some processing are better parsed in the apply functions.
   */
  case class GeneralProperties(published: Option[ZonedDateTime], orbitNumber: Option[Int],
                               organisationName: Option[String], instrument: Option[String],
                               processingBaseline: Option[Double]) {
    def this() = this(None, None, None, None, None)
  }

  def featureBuilder(): FeatureBuilder = FeatureBuilder()

  case class FeatureBuilder private(id: String = "", bbox: Extent = null, nominalDate: ZonedDateTime = null, links: Array[Link]=Array(), resolution: Option[Double] = None,
                                    tileID: Option[String] = None, geometry: Option[Geometry] = None, var crs: Option[CRS] = None,
                                    generalProperties: GeneralProperties = new GeneralProperties(), var rasterExtent: Option[Extent] = None,

                                    var pixelValueOffset: Double = 0    )  {

    def withId(id:String): FeatureBuilder = copy(id=id)
    def withBBox(minx:Double,miny:Double,maxx:Double,maxy:Double): FeatureBuilder = copy(bbox=Extent(minx,miny,maxx,maxy))
    def withNominalDate(nominalDate:String): FeatureBuilder = copy(nominalDate=ZonedDateTime.parse(nominalDate))

    def withResolution(resolution:Double): FeatureBuilder = copy(resolution=Some(resolution))

    def addLink(href:String, title:String, pixelValueOffset:Double, bandNames:java.util.List[String]): FeatureBuilder = {
      val link = Link(URI.create(href), Option(title), Option(pixelValueOffset), Option(bandNames.asScala))
      if(links !=null) {
        copy(links = links :+ link)
      }else{
        copy(links = Array(link))
      }

    }

    def addLink(href:String, title:String, bandNames:java.util.List[String]): FeatureBuilder = {
      val link = Link(URI.create(href), Option(title), None, Option(bandNames.asScala))
      if(links !=null) {
        copy(links = links :+ link)
      }else{
        copy(links = Array(link))
      }

    }

    def withTileId(tileId:String): FeatureBuilder = copy(tileID=Some(tileId))
    def withGeometry(geometry:Geometry): FeatureBuilder = copy(geometry=Some(geometry))

    def withCRS(crs:String): FeatureBuilder = copy(crs=Some(CRS.fromName(crs)))

    def withGeneralProperties(published: Option[ZonedDateTime] = None, orbitNumber: Option[Int] = None,
                             organisationName: Option[String] = None, instrument: Option[String] = None,
                             processingBaseline: Option[Double] = None): FeatureBuilder =
      copy(generalProperties = GeneralProperties(published, orbitNumber, organisationName, instrument, processingBaseline))

    def withRasterExtent( minX:Double, minY:Double, maxX:Double, maxY:Double): FeatureBuilder = copy(rasterExtent=Some(Extent(minX,minY,maxX,maxY)))

    def build(): Feature = Feature(id=id , bbox= bbox , nominalDate= nominalDate , links = links, resolution = resolution, tileID = tileID, geometry = geometry, crs = crs, generalProperties = generalProperties, rasterExtent = rasterExtent, pixelValueOffset = pixelValueOffset)
  }

  case class Feature(id: String, bbox: Extent, nominalDate: ZonedDateTime, links: Array[Link], resolution: Option[Double],
                     tileID: Option[String] = None, geometry: Option[Geometry] = None, var crs: Option[CRS] = None,
                     generalProperties: GeneralProperties = new GeneralProperties(), var rasterExtent: Option[Extent] = None,
                     var pixelValueOffset: Double = 0, // Backwards compatibility. Can probably be removed after openeo-geotrelis-extensions>s2_offset is merged
                    ) {
    if (pixelValueOffset != 0.0) {
      // https://github.com/Open-EO/openeo-geotrellis-extensions/issues/172
      throw new IllegalArgumentException("Use per band based pixelValueOffset instead!")
    }
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

  private def tryToMakeGeometryValid(polygon: Geometry, contextMessage: Option[String] = None): Geometry = {
    if (polygon.isValid) return polygon
    // DouglasPeuckerSimplifier "does not preserve topology", so prefer TopologyPreservingSimplifier:
    val polygonSimplified = TopologyPreservingSimplifier.simplify(polygon, 0.00001)
    if (polygonSimplified.isValid) {
      logger.info("Had to simplify invalid polygon. " + contextMessage.getOrElse(""))
      polygonSimplified
    } else {
      logger.warn("Could not fix invalid polygon. " + contextMessage.getOrElse(""))
      polygon
    }
  }

  def isDuplicate(g1: org.locationtech.jts.geom.Geometry, g2: org.locationtech.jts.geom.Geometry): Boolean = {
    // Do simple check first for performance and robustness
    if (!g1.equalsExact(g2, 0.0001)) {
      val area1 = g1.getArea
      val area2 = g2.getArea

      try {
        val g1Cleaned = tryToMakeGeometryValid(g1)
        val g2Cleaned = tryToMakeGeometryValid(g2)

        val areaIntersect = g1Cleaned.intersection(g2Cleaned).getArea
        // https://en.wikipedia.org/wiki/S%C3%B8rensen%E2%80%93Dice_coefficient
        // The Sørensen–Dice coefficient (see below for other names) is a statistic used to gauge the similarity of two samples
        val diceScore = 2 * areaIntersect / (area1 + area2)
        if (diceScore < 0.99) return false // Threshold is based on gut feeling
      } catch {
        // Polygon intersections can have many edge cases.
        // Errors are probably un-avoidable, so log and go on:
        case e: Throwable =>
          logger.warn("Got error while checking if polygons are duplicate: " + e.toString)
          return false
      }
    }

    true
  }

  private def isDuplicate(f1: Feature, f2: Feature): Boolean = {
    if (ChronoUnit.SECONDS.between(f1.nominalDate, f2.nominalDate) > 30) return false

    // If orbitNumber or organisationName is None it works out too
    if (f1.generalProperties.orbitNumber != f2.generalProperties.orbitNumber) return false
    if (f1.generalProperties.instrument != f2.generalProperties.instrument) return false
    if (f1.resolution.isDefined && f2.resolution.isDefined
      && f1.resolution.get != 0 && f2.resolution.get != 0) {
      if (f1.resolution != f2.resolution) return false
    }

    if (f1.geometry.isDefined && f2.geometry.isDefined) {
      return isDuplicate(f1.geometry.get, f2.geometry.get)
    }

    true
  }

  /**
   * Removes features/products that contain references to PHOEBUS-core files.
   * Those occur often in processing baseline 2.08 products.
   */
  private def removePhoebusFeatures(features: Array[Feature]): Array[Feature] = {
    val newFeatures = features.filter(f => !f.links.exists(
      l => l.href.toString.contains("/PHOEBUS-core/") && l.href.toString.contains("//")
    ))
    if (newFeatures.length < features.length) {
      // This should not be a problem anymore in 2024 Q1
      logger.warn(f"Old, incompatible, features where ignored in this request.")
    }
    newFeatures
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

  // TODO: itemsPerPage is confusing as it's the number of items/features in _this_ page; replace with something like
  //  hasMoreResults: Boolean instead?
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
              }else if(tileId.contains("GLOBE")){
                //CGLS specific convention to set tileId to 'GLOBE'
                Some(LatLng)
              } else {
                None
              }
            }

            var res = resolution
            if (res.isEmpty) {
              // needed for oscars:
              res = c.downField("properties").downField("additionalAttributes").downField("resolution").as[Double].toOption
            }

            if (tileId.contains("GLOBE")) {
              //CGLS sets resolution in meter while it's in fact degrees
              if(res.contains(300)){
                res = Some(0.00297619047620)
              }else if(res.contains(1000)){
                res = Some(0.00892857142857)
              }
            }


            Feature(id, extent, nominalDate, links.values.flatten.toArray, res,
              tileId, geometry = geometry, crs = crs, generalProperties=properties)
          }
        }
      }

      implicit val decodeFeatureCollection: Decoder[FeatureCollection] = new Decoder[FeatureCollection] {
        override def apply(c: HCursor): Decoder.Result[FeatureCollection] = {
          for {
            features <- c.downField("features").as[Array[Feature]]
          } yield {
            val featuresFiltered = if (dedup) dedupFeatures(removePhoebusFeatures(features)) else features
            FeatureCollection(features.length, featuresFiltered)
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
            features <- c.downField("features").as[Array[Feature]]
          } yield {
            val featuresFiltered = if (dedup) dedupFeatures(removePhoebusFeatures(features)) else features
            FeatureCollection(features.length, featuresFiltered)
          }
        }
      }

      decode[FeatureCollection](json)
        .valueOr(e => throw new IllegalArgumentException(s"${e.show} while parsing '$json'", e))
    }
  }

  object CreoFeatureCollection {

    private val logger = LoggerFactory.getLogger(CreoFeatureCollection.getClass)
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
        val retryCondition =
          OrRetryCondition.create(
            RetryCondition.defaultRetryCondition(),
            RetryOnErrorCodeCondition.create("RequestTimeout"),
            RetryOnStatusCodeCondition.create(403),
            RetryOnExceptionsCondition.create(classOf[SocketTimeoutException])
          )
        val backoffStrategy =
          FullJitterBackoffStrategy.builder()
            .baseDelay(Duration.ofMillis(500))
            .maxBackoffTime(Duration.ofMillis(10000))
            .build()
        val retryPolicy =
          RetryPolicy.defaultRetryPolicy()
            .toBuilder()
            .retryCondition(retryCondition)
            .backoffStrategy(backoffStrategy)
            .numRetries(30)
            .build()
        val overrideConfig =
          ClientOverrideConfiguration.builder()
            .apiCallAttemptTimeout(Duration.ofMinutes(10))
            .apiCallTimeout(Duration.ofMinutes(10))
            .retryPolicy(retryPolicy)
            .build()

        Some(S3Client.builder.httpClientBuilder(UrlConnectionHttpClient.builder()
          .socketTimeout(Duration.ofMinutes(1))
          .connectionTimeout(Duration.ofMinutes(1)))
          .endpointOverride(uri).region(Region.of("RegionOne")).overrideConfiguration(overrideConfig)
         .serviceConfiguration(S3Configuration.builder.pathStyleAccessEnabled(true).build).build())

      }else{
        Option.empty
      }
    }

    private def getAwsDirect() = {
      "TRUE".equals(getenv("AWS_DIRECT"))
    }

    /**
     * Can return null!
     * @param pathArg
     * @return
     */
    def loadMetadata(pathArg:String):InputStream = withRetries {
      val path = pathArg.replace("/vsis3/", "/")
      var gdalPrefix = ""
      if (path.startsWith("https://")) {
        gdalPrefix = "/vsicurl"

        val uri = new URI(path)
        uri.resolve(uri.getPath).toURL
          .openConnection.asInstanceOf[java.net.HttpURLConnection]
          .getInputStream
      } else {
        gdalPrefix = if (getAwsDirect()) "/vsis3" else ""

        if (path.startsWith("/eodata")) {

          //reading from /eodata is extremely slow
          if (creoClient.isDefined) {
            val key = path.replace("/eodata/", "")
            try {
              creoClient.get.getObject(GetObjectRequest.builder().bucket("EODATA").key(key).build())
            } catch {

              case _: NoSuchKeyException =>
                logger.error(s"Error reading from S3: " +
                  s"endpoint: " + s3Endpoint + ", " +
                  s"bucket: EODATA, NoSuchKeyException, " +
                  s"key: ${key}"
                )
                null
              case e: Throwable =>
                logger.error(s"Error reading from S3: " +
                  s"endpoint: " + s3Endpoint + ", " +
                  s"bucket: EODATA, " +
                  s"key: ${key}"
                )
                var msgStr = "Error reading from S3 Exception:" + e + "     e.getMessage" + e.getMessage + "     stack: " + e.getStackTraceString
                var cause = e.getCause
                if(cause!=null) {
                  msgStr += "\n Cause: " + cause + "   " + cause.getMessage
                }
                logger.warn(msgStr)
                throw e
            }
          } else {
            val url = path.replace("/eodata", "https://zipper.creodias.eu/get-object?path=")
            val uri = new URI(url)
            try {
              uri.resolve(uri.toString).toURL
                .openConnection.getInputStream
            } catch {
              case e: FileNotFoundException =>
                logger.warn(e.toString)
                null
            }
          }
        } else {
          new FileInputStream(Paths.get(path).toFile)
        }
      }
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
      val inputStream: InputStream = loadMetadata(Paths.get(path, "manifest.safe").toString)
      if(inputStream == null) {
        return Seq.empty[Link]
      }
      val xml = XML.load(inputStream)

      var links = (xml \\ "dataObject" )
        .map((dataObject: Node) =>{
          val title = dataObject \\ "@ID"
          val fileLocation = dataObject \\ "fileLocation" \\ "@href"
          // Fix links in PB 2.08 products:
          val fileLocationString = fileLocation.toString()
            .replaceAll("""/dpc/app/facilities/PHOEBUS-core/PHOEBUS-core[^"]*?/\.S.._OPER[^"]*?/""", "/")
          val filePath =s"$gdalPrefix${if (path.startsWith("/")) "" else "/"}$path" + s"/${URI.create(fileLocationString).normalize().toString}"

          Link(URI.create(filePath), Some(sentinel2Reformat(title.toString,fileLocationString)))
      })

      // https://sentinels.copernicus.eu/web/sentinel/user-guides/sentinel-2-msi/product-types/level-2a
      val metadataUrl = links.find(l => l.title.contains("S2_Level-1C_Product_Metadata") || l.title.contains("S2_Level-2A_Product_Metadata"))
      metadataUrl match {
        case Some(link) =>
          val inputStreamMTD: InputStream = loadMetadata(link.href.toString) // eg: MTD_MSIL2A.xml
          if (inputStreamMTD != null) {
            try {
              val xmlMTD = XML.load(inputStreamMTD)

              def extractNodeText(node: Node) =
                node.child.filter(_.isInstanceOf[scala.xml.Text]).map(_.text).mkString("")

              var offsetNodes = xmlMTD \\ "Radiometric_Offset_List" \\ "RADIO_ADD_OFFSET"
              if (offsetNodes.length > 0) {
                if (offsetNodes.length != 13) {
                  // Did not find documentation for the mapping between band_ids and band names.
                  logger.warn("Unexpected amount of bands. Best to verify pixel value offset.")
                }
                val idToBandList = List(
                  "IMG_DATA_Band_60m_1_Tile1_Data",
                  "IMG_DATA_Band_10m_1_Tile1_Data",
                  "IMG_DATA_Band_10m_2_Tile1_Data",
                  "IMG_DATA_Band_10m_3_Tile1_Data",
                  "IMG_DATA_Band_20m_1_Tile1_Data",
                  "IMG_DATA_Band_20m_2_Tile1_Data",
                  "IMG_DATA_Band_20m_3_Tile1_Data",
                  "IMG_DATA_Band_10m_4_Tile1_Data",
                  "IMG_DATA_Band_20m_4_Tile1_Data",
                  "IMG_DATA_Band_60m_2_Tile1_Data",
                  "IMG_DATA_Band_60m_3_Tile1_Data",
                  "IMG_DATA_Band_20m_5_Tile1_Data",
                  "IMG_DATA_Band_20m_6_Tile1_Data",
                  "IMG_DATA_Band_TCI_Tile1_Data",
                )

                links = links.map(l => {
                  if (l.title.isDefined && idToBandList.contains(l.title.get)) {
                    val bandId = idToBandList.indexOf(l.title.get)
                    offsetNodes.find(p => (p \\ "@band_id").toString() == bandId.toString) match {
                      case Some(node) =>
                        val innerText = extractNodeText(node)
                        l.copy(pixelValueOffset = Some(innerText.toDouble))
                      case _ => l
                    }
                  } else l
                })
              }


              offsetNodes = xmlMTD \\ "BOA_ADD_OFFSET_VALUES_LIST" \\ "BOA_ADD_OFFSET"
              if (offsetNodes.length > 0) {
                if (offsetNodes.length != 13) {
                  // Did not find documentation for the mapping between band_ids and band names.
                  logger.warn("Unexpected amount of bands. Best to verify pixel value offset.")
                }
                val idToBandList = List(
                  "IMG_DATA_Band_B01_60m_Tile1_Data",
                  "IMG_DATA_Band_B02_10m_Tile1_Data",
                  "IMG_DATA_Band_B03_10m_Tile1_Data",
                  "IMG_DATA_Band_B04_10m_Tile1_Data",
                  "IMG_DATA_Band_B05_20m_Tile1_Data",
                  "IMG_DATA_Band_B06_20m_Tile1_Data",
                  "IMG_DATA_Band_B07_20m_Tile1_Data",
                  "IMG_DATA_Band_B08_10m_Tile1_Data",
                  "IMG_DATA_Band_B8A_20m_Tile1_Data",
                  "IMG_DATA_Band_B09_60m_Tile1_Data",
                  "IMG_DATA_Band_B10_60m_Tile1_Data", // Never occurs, but needed to have 13 bands. TODO: Verify on staging.
                  "IMG_DATA_Band_B11_20m_Tile1_Data",
                  "IMG_DATA_Band_B12_20m_Tile1_Data",
                  "IMG_DATA_Band_TCI_10m_Tile1_Data",
                  "IMG_DATA_Band_WVP_10m_Tile1_Data",
                  "IMG_DATA_Band_AOT_20m_Tile1_Data",
                  "IMG_DATA_Band_SCL_20m_Tile1_Data",
                )

                links = links.map(l => {
                  if (l.title.isDefined && idToBandList.contains(l.title.get)) {
                    val bandId = idToBandList.indexOf(l.title.get)
                    offsetNodes.find(p => (p \\ "@band_id").toString() == bandId.toString) match {
                      case Some(node) =>
                        val innerText = extractNodeText(node)
                        l.copy(pixelValueOffset = Some(innerText.toDouble))
                      case _ => l
                    }
                  } else l
                })
              }
            } catch {
              // This occured in mocked automatic tests. I did not see it on the real data yet.
              case e: Throwable => logger.warn("Failed to load " + link.href.toString +
                ". Error: " + e.getMessage)
            } finally {
              inputStreamMTD.close()
            }
          }
        case _ =>
      }

      links
    }

    /**
     * Creo catalogs do not point to the actual file, so we need to custom lookups
     * @param path
     * @return
     */
    private def getDEMPathFromInspire(path: String): Seq[Link] = {
      val gdalPrefix: String = getGDALPrefix(path)
      val inputStream: InputStream = loadMetadata(Paths.get(path, "INSPIRE.xml").toString)
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

    private def getLandsat8FilePaths(path: String): Seq[Link] = {
      val commonFilePrefix = path.split("/").last
      val metadataXmlPath = s"${getGDALPrefix(path)}$path/${commonFilePrefix}_MTL.xml"
      val fileNameNodeLabelPrefix = "FILE_NAME_"

      val links = Using(loadMetadata(metadataXmlPath)) { in =>
        val metadataXml = XML.load(in)

        for {
          productContents <- metadataXml \\ "LANDSAT_METADATA_FILE" \ "PRODUCT_CONTENTS"
          fileName <- productContents.child if fileName.label.startsWith(fileNameNodeLabelPrefix)
        } yield Link(
          href = URI.create(s"${getGDALPrefix(path)}$path/${fileName.text}"),
          title = Some(fileName.label.drop(fileNameNodeLabelPrefix.length))
        )
      }

      links.get
    }

    private def getSentinel1RTCFilePaths(path: String): Seq[Link] = {
      val commonFilePrefix = path.split("/").last

      val parts = commonFilePrefix.split("_")
      val prefix = s"s1_rtc_${parts.last}_${parts.dropRight(1).mkString("_")}"
      val suffixes = Seq("ANGLE", "AREA","MASK","VH","VV")

      suffixes.map(s=>Link(URI.create(s"${getGDALPrefix(path)}$path/${prefix}_$s.tif"),Some(s)))
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

    def parse(json: String, dedup: Boolean = false, tileIdPattern: Option[String] = None): FeatureCollection = {
      implicit val decodeFeature: Decoder[Feature] = new Decoder[Feature] {
        override def apply(c: HCursor): Decoder.Result[Feature] = {
          for {
            id <- c.downField("properties").downField("productIdentifier").as[String] // TODO: that's not the feature ID
            geometry <- c.downField("geometry").as[Json]
            nominalDate <- c.downField("properties").downField("startDate").as[ZonedDateTime]
            links <- c.downField("properties").downField("links").as[Array[Link]]
            resolution = c.downField("properties").downField("resolution").as[Double].toOption
            properties <- c.downField("properties").as[GeneralProperties]
          } yield {
            val theGeometry = tryToMakeGeometryValid(ensureValidGeometry(geometry).toString().parseGeoJson[Geometry], Some(id))
            val extent = theGeometry.extent
            val tileIDMatcher = TILE_PATTERN.matcher(id)
            val tileID =
            if(tileIDMatcher.find()){
              Some(tileIDMatcher.group(1))
            }else{
              Option.empty
            }

            if (id.endsWith(".SAFE") || id.startsWith("/eodata/Sentinel-2/MSI/")) {
              val all_links = getFilePathsFromManifest(id)
              Feature(id, extent, nominalDate, all_links.toArray, resolution, tileID, Option(theGeometry), generalProperties = properties)
            } else if (id.contains("COP-DEM_GLO-30-DGED")) {
              val all_links = getDEMPathFromInspire(id)
              Feature(id, extent, nominalDate, all_links.toArray, resolution, tileID, Option(theGeometry), generalProperties = properties)
            } else if (id.startsWith("/eodata/Landsat-8/OLI_TIRS")) {
              Feature(id, extent, nominalDate, getLandsat8FilePaths(path = id).toArray, resolution, tileID, Some(theGeometry), generalProperties = properties)
            } else if (id.startsWith("/eodata/Sentinel-1-RTC")) {
              Feature(id, extent, nominalDate, getSentinel1RTCFilePaths(path = id).toArray, resolution, tileID, Some(theGeometry), generalProperties = properties)
            } else {
              Feature(id, extent, nominalDate, links, resolution, tileID, Option(theGeometry), generalProperties = properties)
            }
          }
        }
      }

      implicit val decodeFeatureCollection: Decoder[FeatureCollection] = new Decoder[FeatureCollection] {
        override def apply(c: HCursor): Decoder.Result[FeatureCollection] = {
          for {
            features <- c.downField("features").as[Array[Feature]]
          } yield {
            val featuresFiltered =
              if (dedup) dedupFeatures(removePhoebusFeatures(retainTileIdPattern(features, tileIdPattern)))
              else retainTileIdPattern(features, tileIdPattern)
            FeatureCollection(features.length, featuresFiltered)
          }
        }
      }

      decode[FeatureCollection](json)
        .valueOr(e => throw new IllegalArgumentException(s"${e.show} while parsing '$json'", e))
    }

    private def retainTileIdPattern(features: Array[Feature], tileIdPattern: Option[String]): Array[Feature] =
      tileIdPattern match {
        case Some(pattern) => features.filter(feature => feature.tileID match {
          case Some(tileId) =>
            val matchesPattern = tileId matches pattern.replace("*", ".*")
            logger.debug(s"${if (matchesPattern) "retaining" else "omitting"} feature ${feature.id} with tileId $tileId")
            matchesPattern
          case _ =>
            logger.warn(s"omitting feature ${feature.id} with unknown tileId")
            false
        })
        case _ => features
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
