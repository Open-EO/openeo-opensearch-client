package org.openeo

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector.Extent
import org.junit.Assert._
import org.junit.Test
import org.openeo.TestHelpers.loadJsonResource
import org.openeo.opensearch.OpenSearchResponses
import org.openeo.opensearch.OpenSearchResponses.{FeatureCollection, STACFeatureCollection}

import java.io.{PrintWriter, StringWriter}
import java.net.URI
import java.time.ZonedDateTime

class OpenSearchResponsesTest {

  @Test
  def testFeatureBuilder(): Unit = {
    val f = OpenSearchResponses.FeatureBuilder()
      .withBBox(0, 0, 10, 10)
      .withId("id")
      .withNominalDate("2021-01-01T00:00:00Z")
      .addLink("url", "title", 0.0, java.util.Arrays.asList("B01", "B02"))
      .withCRS("EPSG:32631")
      .withGeometryFromWkt("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))")
      .build()

    assertEquals("id", f.id)
    assertEquals(Extent(0, 0, 10, 10), f.bbox)
    assertEquals(ZonedDateTime.parse("2021-01-01T00:00:00Z"), f.nominalDate)
    assertEquals(1, f.links.length)
    assertEquals("url", f.links(0).href.toString)
    assertEquals("title", f.links(0).title.get)
    assertEquals(0.0, f.links(0).pixelValueOffset.get, 0.0)
    assertEquals("EPSG:32631", f.crs.get.toString())
    assertEquals(Some(Extent(0, 0, 10, 10).toPolygon()), f.geometry)
  }

  @Test
  def testReformat(): Unit = {
    assertEquals("IMG_DATA_Band_B8A_20m_Tile1_Data", OpenSearchResponses.sentinel2Reformat("IMG_DATA_20m_Band9_Tile1_Data", "GRANULE/L2A_T30SVH_A017537_20181031T110435/IMG_DATA/R20m/T30SVH_20181031T110201_B8A_20m.jp2"))
    assertEquals("IMG_DATA_Band_B12_60m_Tile1_Data", OpenSearchResponses.sentinel2Reformat("IMG_DATA_60m_Band10_Tile1_Data", "GRANULE/L2A_T30SVH_A017537_20181031T110435/IMG_DATA/R60m/T30SVH_20181031T110201_B12_60m.jp2"))
    assertEquals("IMG_DATA_Band_SCL_60m_Tile1_Data", OpenSearchResponses.sentinel2Reformat("SCL_DATA_60m_Tile1_Data", "GRANULE/L2A_T30SVH_A017537_20181031T110435/IMG_DATA/R60m/T30SVH_20181031T110201_SCL_60m.jp2"))
    assertEquals("IMG_DATA_Band_SCL_60m_Tile1_Data", OpenSearchResponses.sentinel2Reformat("SCL_DATA_60m_Tile1_Data", "GRANULE/L2A_T30SVH_A017537_20181031T110435/IMG_DATA/R60m/T30SVH_20181031T110201_SCL_60m.jp2"))
  }

  @Test
  def parseSTACItemsResponse(): Unit = {
    parseSTACItemsResponse(s3URL = false, "https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/44/N/ML/2020/12/S2A_44NML_20201218_0_L2A/SCL.tif")
  }

  @Test
  def parseSTACItemsResponseS3(): Unit = {
    parseSTACItemsResponse(s3URL = true, "s3://sentinel-cogs/sentinel-s2-l2a-cogs/44/N/ML/2020/12/S2A_44NML_20201218_0_L2A/SCL.tif")
  }

  def parseSTACItemsResponse(s3URL: Boolean, expectedURL: String): Unit = {
    val productsResponse = loadJsonResource("stacItemsResponse.json")
    val features = STACFeatureCollection.parse(productsResponse, s3URL).features
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
    assertEquals(CRS.fromEpsgCode(32736), features.head.crs.get)

    assertTrue(features.exists(_.geometry.isDefined))
    assertEquals(ZonedDateTime.parse("2020-01-31T14:58:33Z"),
      features.find(_.geometry.isDefined).get.generalProperties.published.get)

    val Some(dataUrl) = features.head.links
      .find(_.title contains "SCENECLASSIFICATION_20M")
      .map(_.href)

    assertEquals(new URI("https://oscars-dev.vgt.vito.be/download" +
      "/CGS_S2_FAPAR/2019/11/28/S2A_20191128T075251Z_36MZE_CGS_V102_000/S2A_20191128T075251Z_36MZE_FAPAR_V102/10M" +
      "/S2A_20191128T075251Z_36MZE_SCENECLASSIFICATION_20M_V102.tif"), dataUrl)

    assertEquals(LatLng, features(1).crs.get)
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
    val features = FeatureCollection.parse(collectionsResponse, dedup = false).features

    assertEquals(8, features.length)

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
  def parseOscarsIssue6(): Unit = {
    val collectionsResponse = loadJsonResource("oscarsIssue6.json")
    val features = FeatureCollection.parse(collectionsResponse, dedup = true).features

    // Dedup will remove 'urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20180814T105019_31UFS_TOC_V200'
    assertEquals(1, features.length)

    assertEquals("urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20180814T105019_31UFS_TOC_V210", features(0).id)
  }

  @Test
  def parseIncompleteResponseExceptionContainsUsefulInformation(): Unit = {
    val productsResponse = loadJsonResource("oscarsProductsResponse.json")
    val incompleteResponse = productsResponse.take(productsResponse.length / 2)

    try FeatureCollection.parse(incompleteResponse)
    catch {
      case e: Throwable =>
        assertTrue(e.getMessage, e.getMessage contains """"type": "FeatureCollection"""")

        val stackTrace = this.stackTrace(e)
        assertTrue(stackTrace, stackTrace contains getClass.getName)
    }
  }

  @Test
  def parseOscarsResolutionDifference(): Unit = {
    // https://services.terrascope.be/catalogue/products?collection=urn%3Aeop%3AVITO%3ATERRASCOPE_S2_FAPAR_V2&bbox=5.083186899526165%2C51.19551975508295%2C5.089390013994452%2C51.19765894962577&sortKeys=title&startIndex=1&accessedFrom=MEP&clientId=correlationid_2&start=2019-03-07T00%3A00%3A00Z&end=2019-03-07T23%3A59%3A59.999999999Z
    val productsResponse = loadJsonResource("oscarsResolutionDifference.json")
    val features = FeatureCollection.parse(productsResponse, dedup = false).features
    assertEquals(4, features.length)

    val featuresDedup = FeatureCollection.parse(productsResponse, dedup = true).features
    assertEquals(2, featuresDedup.length)
  }

  @Test
  def parseFaultyResponseExceptionContainsStackTraceContainsUsefulInformation(): Unit = {
    val productsResponse = loadJsonResource("oscarsProductsResponse.json")
    val faultyResponse = productsResponse.replace("features", "featurez")

    try FeatureCollection.parse(faultyResponse)
    catch {
      case e: Throwable =>
        assertTrue(e.getMessage, e.getMessage contains """"type": "FeatureCollection"""")

        val stackTrace = this.stackTrace(e)
        assertTrue(stackTrace, stackTrace contains getClass.getName)
    }
  }

  @Test
  def latlonResponse(): Unit = {
    val productsResponse = loadJsonResource("latlonResponse.json")
    val features = FeatureCollection.parse(productsResponse, dedup = true).features
    assertEquals(1, features.length)
    assertEquals(LatLng, features(0).crs.get)
    assertEquals(features(0).rasterExtent.get, features(0).bbox)
  }

  private def stackTrace(e: Throwable): String = {
    val s = new StringWriter
    val out = new PrintWriter(s)

    try e.printStackTrace(out)
    finally out.close()

    s.toString
  }
}
