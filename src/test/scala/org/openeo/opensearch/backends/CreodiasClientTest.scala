package org.openeo.opensearch.backends

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.jupiter.api.Test

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
}
