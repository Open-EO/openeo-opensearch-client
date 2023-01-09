package org.openeo

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector.Extent
import org.junit.Assert._
import org.junit.Test
import org.openeo.opensearch.OpenSearchResponses.{CreoFeatureCollection, FeatureCollection, STACFeatureCollection}

import java.io.{PrintWriter, StringWriter}
import java.net.URI
import java.time.ZonedDateTime
import scala.io.{Codec, Source}

class OpenSearchResponsesTest {

  private val resourcePath = "/org/openeo/"

  private def loadJsonResource(classPathResourceName: String, codec: Codec = Codec.UTF8): String = {
    val fullPath = resourcePath + classPathResourceName
    val jsonFile = Source.fromURL(getClass.getResource(fullPath))(codec)

    try jsonFile.mkString
    finally jsonFile.close()
  }

  @Test
  def parseSTACItemsResponse(): Unit = {
    parseSTACItemsResponse(false,"https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/44/N/ML/2020/12/S2A_44NML_20201218_0_L2A/SCL.tif")
  }

  @Test
  def parseSTACItemsResponseS3(): Unit = {
    parseSTACItemsResponse(true,"s3://sentinel-cogs/sentinel-s2-l2a-cogs/44/N/ML/2020/12/S2A_44NML_20201218_0_L2A/SCL.tif")
  }

  def parseSTACItemsResponse(s3URL:Boolean, expectedURL:String): Unit = {
    val productsResponse = loadJsonResource("stacItemsResponse.json")
    val features = STACFeatureCollection.parse(productsResponse,s3URL).features
    assertEquals(1, features.length)

    assertEquals(Extent(80.15231456198393, 5.200107055229471, 81.08809406509769, 5.428209952833148), features.head.bbox)


    val Some(dataUrl) = features.head.links
      .find(_.title.getOrElse("") contains "SCL")
      .map(_.href)

    assertEquals(URI.create(expectedURL), dataUrl)
  }

  @Test
  def parseProductsResponse(): Unit = {
    val productsResponse = loadJsonResource("oscarsProductsResponse.json")
    val features = FeatureCollection.parse(productsResponse).features

    assertEquals(2, features.length)
    assertEquals(Extent(35.6948436874, -0.991331687854, 36.6805874343, 0), features.head.bbox)
    assertEquals("36MZE", features.head.tileID.get)
    assertEquals(CRS.fromEpsgCode(32736),features.head.crs.get)

    assertTrue(features.exists(_.geometry.isDefined))
    assertEquals(ZonedDateTime.parse("2020-01-31T14:58:33Z"),
      features.find(_.geometry.isDefined).get.generalProperties.published.get)

    val Some(dataUrl) = features.head.links
      .find(_.title contains "SCENECLASSIFICATION_20M")
      .map(_.href)

    assertEquals(new URI("https://oscars-dev.vgt.vito.be/download" +
                           "/CGS_S2_FAPAR/2019/11/28/S2A_20191128T075251Z_36MZE_CGS_V102_000/S2A_20191128T075251Z_36MZE_FAPAR_V102/10M" +
                           "/S2A_20191128T075251Z_36MZE_SCENECLASSIFICATION_20M_V102.tif"), dataUrl)

    assertEquals(LatLng,features(1).crs.get)
  }

  @Test
  def parseNoProductsResponse(): Unit = {
    val productsResponse = loadJsonResource("oscarsNoProductsResponse.json")
    val features = FeatureCollection.parse(productsResponse).features

    assertEquals(0, features.length)
  }

  @Test
  def parseCreodiasDuppedFeature(): Unit = {
    val collectionsResponse = loadJsonResource("creodiasDuppedFeature.json")
    val features = CreoFeatureCollection.parse(collectionsResponse).features

    assertEquals(1, features.length)

    val feature = features(0)

    // Check if we really picked the latest Feature:
    assertEquals(ZonedDateTime.parse("2021-04-11T08:51:07.054814Z"), feature.generalProperties.published.get)
    assertEquals("/eodata/Sentinel-1/SAR/GRD/2021/04/11/S1B_IW_GRDH_1SDV_20210411T054146_20210411T054211_026415_032740_6184.SAFE", feature.id)
  }

  @Test
  def parseCreodiasSpecialDupped(): Unit = {
    val creodiasFeatureSnippet = loadJsonResource("creodiasFeatureSnippet.json")
    val composedJsonString = """{
      |  "type": "FeatureCollection",
      |  "properties": {
      |    "id": "fad6ca4a-3ab6-59e5-8e69-14aaf70256de",
      |    "totalResults": 2,
      |    "exactCount": true,
      |    "startIndex": 1,
      |    "itemsPerPage": 2,
      |    "links": [
      |      {
      |        "rel": "self",
      |        "type": "application/json",
      |        "title": "self",
      |        "href": "https://finder.creodias.eu/resto/api/collections/Sentinel1/search.json?&maxRecords=10&startDate=2021-04-11T00%3A00%3A00Z&completionDate=2021-04-11T23%3A59%3A59Z&productType=GRD&sensorMode=IW&geometry=POLYGON%28%285.785404630537803%2051.033953432779526%2C5.787426293119076%2051.021746940265956%2C5.803195261253003%2051.018694814851074%2C5.803195261253003%2051.02912208053834%2C5.785404630537803%2051.033953432779526%29%29&sortParam=startDate&sortOrder=descending&status=all&dataset=ESA-DATASET"
      |      },
      |      {
      |        "rel": "search",
      |        "type": "application/opensearchdescription+xml",
      |        "title": "OpenSearch Description Document",
      |        "href": "https://finder.creodias.eu/resto/api/collections/Sentinel1/describe.xml"
      |      }
      |    ]
      |  },
      |  "features": [""".stripMargin +
      creodiasFeatureSnippet.replaceAll("%startDate%", "2000-01-01T01:01:01.001Z") + ", \n" +
      creodiasFeatureSnippet.replaceAll("%startDate%", "2000-01-01T01:01:29.001Z") + ", \n" +
      creodiasFeatureSnippet.replaceAll("%startDate%", "2000-01-01T01:01:32.001Z") +
      "]}"
    val features = CreoFeatureCollection.parse(composedJsonString).features

    // Even tough the images are pairwise equal, the first and the last are not.
    // So the dedup code is forced to consider them as separate
    assertEquals(2, features.length)
  }

  @Test
  def parseCreodiasDifferentGeom(): Unit = {
    val collectionsResponse = loadJsonResource("creodiasDifferentGeom.json")
    val features = CreoFeatureCollection.parse(collectionsResponse).features

    assertEquals(3, features.length)
  }

  @Test
  def parseCollectionsResponse(): Unit = {
    val collectionsResponse = loadJsonResource("oscarsCollectionsResponse.json")
    val features = FeatureCollection.parse(collectionsResponse).features

    // Dedup will remove 'urn:eop:VITO:CGS_S2_RAD_L2_V200'
    assertEquals(7, features.length)

    // Order should be kept
    assertEquals("urn:eop:VITO:CGS_S2_FAPAR", features(0).id)
    assertEquals("urn:eop:VITO:CGS_S2_RAD_L2", features(1).id)
    assertEquals("urn:ogc:def:EOP:VITO:PROBAV_L2A_1KM_V001", features(2).id)
    assertEquals("urn:ogc:def:EOP:VITO:PROBAV_S1-TOC_333M_V001", features(3).id)

    val Some(s2Fapar) = features
      .find(_.id == "urn:eop:VITO:CGS_S2_FAPAR")

    assertEquals(Extent(-179.999, -84, 179.999, 84), s2Fapar.bbox)
  }

  @Test
  def parseIncompleteResponseExceptionContainsUsefulInformation(): Unit = {
    val productsResponse = loadJsonResource("oscarsProductsResponse.json")
    val incompleteResponse = productsResponse.take(productsResponse.length / 2)

    try FeatureCollection.parse(incompleteResponse)
    catch {
      case e =>
        assertTrue(e.getMessage, e.getMessage contains """"type": "FeatureCollection"""")

        val stackTrace = this.stackTrace(e)
        assertTrue(stackTrace, stackTrace contains getClass.getName)
    }
  }

  @Test
  def parseFaultyResponseExceptionContainsStackTraceContainsUsefulInformation(): Unit = {
    val productsResponse = loadJsonResource("oscarsProductsResponse.json")
    val faultyResponse = productsResponse.replace("features", "featurez")

    try FeatureCollection.parse(faultyResponse)
    catch {
      case e =>
        assertTrue(e.getMessage, e.getMessage contains """"type": "FeatureCollection"""")

        val stackTrace = this.stackTrace(e)
        assertTrue(stackTrace, stackTrace contains getClass.getName)
    }
  }

  private def stackTrace(e: Throwable): String = {
    val s = new StringWriter
    val out = new PrintWriter(s)

    try e.printStackTrace(out)
    finally out.close()

    s.toString
  }
}