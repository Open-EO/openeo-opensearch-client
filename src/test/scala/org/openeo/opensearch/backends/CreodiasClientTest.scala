package org.openeo.opensearch.backends

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector.{Extent, ProjectedExtent}
import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.openeo.opensearch.to_0_360_range

import java.time.LocalDate

class CreodiasClientTest {

  @Test
  def testSuitableAsCacheKey(): Unit =
    EqualsVerifier.forClass(classOf[CreodiasClient])
      .withNonnullFields("endpoint")
      .verify()

  @Test
  def testGetProducts(): Unit = {
    val client = CreodiasClient()

    client.getProducts(
      "Sentinel1",
      dateRange = (LocalDate.of(2017, 3, 5), LocalDate.of(2017 ,3, 10)),
      bbox = ProjectedExtent(Extent(3.7496741657535795, 51.28966910932136, 3.8003477052967813, 51.310330842158784), LatLng),
      attributeValues = Map[String, Any](),
      correlationId = "",
      processingLevel = ""
    )
  }

  @Test
  def testGetProductsAntimeridian(): Unit = {
    val client = CreodiasClient()

    val features = client.getProducts(
      "GLOBAL-MOSAICS",
      dateRange = (LocalDate.of(2020, 1, 1), LocalDate.of(2020, 3, 2)),
      bbox = ProjectedExtent(Extent(300000, 7703320, 409800, 7800000), CRS.fromEpsgCode(32601)),
      attributeValues = Map[String, Any](),
      correlationId = "",
      processingLevel = ""
    )
    for (feature <- features) {
      // Check if the returnd products are close to the anitmeridian
      assertTrue(Math.abs(to_0_360_range(feature.bbox.xmin) - 180) < 10)
    }
  }
}
