package org.openeo

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.openeo.opensearch.OpenSearchResponses
import org.openeo.opensearch.backends.CreodiasClient

import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}
import scala.collection.Map

class CreodiasAPITest {

  private var httpsCacheEnabledOriginalValue = false

  @BeforeEach def beforeEach(): Unit = {
    httpsCacheEnabledOriginalValue = HttpCache.enabled
  }

  @AfterEach def afterEach(): Unit = {
    HttpCache.enabled = httpsCacheEnabledOriginalValue
  }

  @Test
  def testCreoGeometry(): Unit = {
    HttpCache.enabled = true
    /*
    The old Creodias API can sometimes return MultiPolygon geometry that is not correctly formatted.
    */

    // Send a request to Creodias API that results in wrongly formatted multipolygons.
    val fromDate: ZonedDateTime = ZonedDateTime.of(2020, 6, 6, 0, 0, 0, 0, UTC)
    val toDate: ZonedDateTime = ZonedDateTime.of(2020, 6, 6, 23, 59, 59, 999999999, UTC)
    val crs: CRS = CRS.fromName("EPSG:32631")
    val bbox = ProjectedExtent(Extent(506961.00315656577, 5679855.354590951, 520928.44779978535, 5691014.304235179), crs)
    val attributeValues = Map[String, Any]("processingLevel" -> "LEVEL1", "sensorMode" -> "IW", "productType" -> "GRD")

    // The parser in getProducts should be able to handle these incorrect multipolygons.
    val features1 = new CreodiasClient().getProducts(
      "Sentinel1", Some(fromDate, toDate),
      bbox, attributeValues
    )
    assertEquals(2, features1.size)
    val features2 = new CreodiasClient(allowParallelQuery = true).getProducts(
      "Sentinel1", Some(fromDate, toDate),
      bbox, attributeValues
    )
    assertEquals(2, features2.size)
  }

  @Test
  def testTileIdWithWildcard(): Unit = {
    def getProducts(tileIdPattern: Option[String]): Seq[OpenSearchResponses.Feature] = {
      new CreodiasClient().getProducts(
        "Sentinel2",
        dateRange = LocalDate.of(2023, 9, 24) -> LocalDate.of(2023, 9, 24),
        bbox = ProjectedExtent(
          Extent(4.912844218500582, 51.02816932187383, 4.918160603369832, 51.029815337603594), LatLng),
        attributeValues = tileIdPattern.map("tileId" -> _).toMap,
        correlationId = "",
        processingLevel = ""
      )
    }

    assertTrue(getProducts(tileIdPattern = None).nonEmpty) // sanity check
    assertTrue(getProducts(tileIdPattern = Some("30*")).isEmpty)
  }

  @Test
  def testLargeTemporalExtent(): Unit = {
    HttpCache.enabled = true

    val extentTAP4326 = Extent(5.07, 51.215, 5.08, 51.22)
    def getProducts(tileIdPattern: Option[String]): Seq[OpenSearchResponses.Feature] = {
      new CreodiasClient().getProducts(
        "Sentinel2",
        dateRange = LocalDate.of(2016, 1, 1) -> LocalDate.of(2023, 12, 1),
        bbox = ProjectedExtent(extentTAP4326, LatLng),
        attributeValues = Map[String, Any](),
        correlationId = "",
        processingLevel = ""
      )
    }

    assertTrue(getProducts(tileIdPattern = None).nonEmpty) // sanity check
  }
}
