package org.openeo

import java.io.{PrintWriter, StringWriter}
import java.net.URI

import org.openeo.opensearch.OpenSearchResponses.{FeatureCollection, STACFeatureCollection}
import geotrellis.proj4.CRS
import geotrellis.vector.Extent
import org.junit.Assert._
import org.junit.Test

import scala.io.{Codec, Source}

class OpenSearchResponsesTest {

  private val resourcePath = "/be/vito/eodata/gwcgeotrellis/opensearch/"

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

    assertEquals(1, features.length)
    assertEquals(Extent(35.6948436874, -0.991331687854, 36.6805874343, 0), features.head.bbox)
    assertEquals("36MZE", features.head.tileID.get)
    assertEquals(CRS.fromEpsgCode(32736),features.head.crs.get)

    val Some(dataUrl) = features.head.links
      .find(_.title contains "SCENECLASSIFICATION_20M")
      .map(_.href)

    assertEquals(new URI("https://oscars-dev.vgt.vito.be/download" +
                           "/CGS_S2_FAPAR/2019/11/28/S2A_20191128T075251Z_36MZE_CGS_V102_000/S2A_20191128T075251Z_36MZE_FAPAR_V102/10M" +
                           "/S2A_20191128T075251Z_36MZE_SCENECLASSIFICATION_20M_V102.tif"), dataUrl)
  }

  @Test
  def parseNoProductsResponse(): Unit = {
    val productsResponse = loadJsonResource("oscarsNoProductsResponse.json")
    val features = FeatureCollection.parse(productsResponse).features

    assertEquals(0, features.length)
  }

  @Test
  def parseCollectionsResponse(): Unit = {
    val collectionsResponse = loadJsonResource("oscarsCollectionsResponse.json")
    val features = FeatureCollection.parse(collectionsResponse).features

    assertEquals(8, features.length)

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