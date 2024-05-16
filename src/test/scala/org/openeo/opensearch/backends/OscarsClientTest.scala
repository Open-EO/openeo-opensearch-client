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
      .verify()

  @Test
  def testGDMP(): Unit = {
    val openSearch = OpenSearchClient("https://globalland.vito.be/catalogue", isUTM = false, dateRegex = "",
      bands = Collections.singletonList("GDMP"), clientType = "cgls_oscars", false)
    val collections = openSearch.getCollections("testGDMP")
    assert(collections.length > 10) // by default a page is 100
    val unique = collections.map(_.id).toSet
    assertEquals(collections.length, unique.size) // assure no duplicates
    print(collections)

    val features = openSearch.getProducts(
      collectionId = "clms_global_gdmp_300m_v1_10daily_netcdf",
      (LocalDate.of(2018, 8, 10), LocalDate.of(2018, 8, 13)),
      ProjectedExtent(Extent(-3.7937789378418247, 38.486414764328515, -3.5314443712734733, 38.69684729114566), LatLng),
      Map[String, Any]( "accessedFrom" -> "MEP"), correlationId = "testGDMP", processingLevel = null
    )
    println(features)
    assertEquals(Some(LatLng),features.head.crs)
    assertEquals(Some(0.00297619047620),features.head.resolution)
    val actual = features.head.links.head.href.toString
      .replace("-RT5_", "-RT2_")
    assertEquals("NETCDF:/data/MTDA/Copernicus/Land/global/netcdf/dry_matter_productivity/gdmp_300m_v1_10daily/2018/20180810/c_gls_GDMP300-RT2_201808100000_GLOBE_PROBAV_V1.0.1.nc:GDMP", actual)
  }

  @Test
  def testGDMP1KM(): Unit = {
    val openSearch = OpenSearchClient(new URL("https://globalland.vito.be/catalogue"))

    val features = openSearch.getProducts(
      collectionId = "clms_global_gdmp_1km_v2_10daily_netcdf",
      (LocalDate.of(2018, 8, 10), LocalDate.of(2018, 8, 13)),
      ProjectedExtent(Extent(-3.7937789378418247, 38.486414764328515, -3.5314443712734733, 38.69684729114566), LatLng),
      Map[String, Any]("accessedFrom" -> "MEP"), correlationId = "testGDMP", processingLevel = null
    )
    println(features)
    assertEquals(Some(LatLng), features.head.crs)
    assertEquals(Some(0.00892857142857), features.head.resolution)
  }

  @Test
  def testPagination(): Unit = {
    val openSearch = OpenSearchClient(new URL("https://services.terrascope.be/catalogue"))
    val collections = openSearch.getCollections("testPagination")
    assert(collections.length > 100) // by default a page is 100
    val unique = collections.map(_.id).toSet
    assertEquals(collections.length, unique.size) // assure no duplicates
    print(collections)
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
