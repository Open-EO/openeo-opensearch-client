package org.openeo

import geotrellis.proj4.CRS
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.jupiter.api.Test
import org.openeo.opensearch.backends.CreodiasClient

import java.time.ZoneOffset.UTC
import java.time.ZonedDateTime
import scala.collection.Map

class CreodiasAPITest {

  @Test
  def testCreoGeometry(): Unit = {
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
    new CreodiasClient().getProducts(
      "Sentinel1", Some(fromDate, toDate),
      bbox, attributeValues, "", ""
    )
  }

}
