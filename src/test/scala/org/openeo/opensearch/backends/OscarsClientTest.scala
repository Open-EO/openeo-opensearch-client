package org.openeo.opensearch.backends

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.openeo.opensearch.OpenSearchClient

import java.net.URL
import java.time.LocalDate
import java.util.Collections
import java.util.stream.{Stream => JStream}
import scala.collection.Map

object OscarsClientTest {
  def sentinel2TileIdAttributeValues: JStream[Arguments] = JStream.of(
    Arguments.of(Map[String, Any](), Set("31UES", "31UET", "31UFS", "31UFT")),
    Arguments.of(Map("tileId" -> "31UFS"), Set("31UFS")),
    Arguments.of(Map("tileId" -> java.util.Arrays.asList("31UES", "31UFS")), Set("31UES", "31UFS")),
  )
}

class OscarsClientTest {

  @Test
  def testSuitableAsCacheKey(): Unit =
    EqualsVerifier.forClass(classOf[OscarsClient])
      .withNonnullFields("endpoint")
      .withNonnullFields("deduplicationPropertyJsonPath")
      .verify()

  @Test
  def testPagination(): Unit = {
    val openSearch = OpenSearchClient(new URL("https://services.terrascope.be/catalogue"))
    val collections = openSearch.getCollections("testPagination")
    assert(collections.length > 100) // by default a page is 100
    val unique = collections.map(_.id).toSet
    assertEquals(collections.length, unique.size) // assure no duplicates
    println(collections)
  }

  @ParameterizedTest
  @MethodSource(Array("sentinel2TileIdAttributeValues"))
  def testFilterByMultipleTileIds(attributeValues: Map[String, Any], expectedTileIds: Set[String]): Unit = {
    val openSearch = OpenSearchClient(new URL("https://services.terrascope.be/catalogue"))

    val features = openSearch.getProducts(
      collectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      dateRange = (LocalDate.of(2024, 4, 24), LocalDate.of(2024, 4, 25)),
      bbox = ProjectedExtent(Extent(
        4.4158740490713804, 51.4204485519121945,
        4.4613941769140322, 51.4639210615473885), LatLng),
      attributeValues = attributeValues,
      correlationId = "testFilterByMultipleTileIds",
      processingLevel = null,
    )

    val actualTileIds = features.flatMap(_.tileID).toSet

    assertEquals(expectedTileIds, actualTileIds)
  }
}
