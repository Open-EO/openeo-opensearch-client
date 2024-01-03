package org.openeo.opensearch.backends

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.openeo.opensearch.OpenSearchClient

import java.net.URL
import java.time.LocalDate
import scala.collection.Map

class OscarsClientTest {

  @Test
  def testSuitableAsCacheKey(): Unit =
    EqualsVerifier.forClass(classOf[OscarsClient])
      .withNonnullFields("endpoint")
      .verify()

  @Test
  def testGDMP(): Unit = {
    val openSearch = OpenSearchClient(new URL("https://globalland.vito.be/catalogue"))
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
}
