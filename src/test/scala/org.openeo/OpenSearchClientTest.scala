package org.openeo

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.Assert._
import org.junit.{Ignore, Test}
import org.openeo.opensearch.OpenSearchClient
import org.openeo.opensearch.backends.{CreodiasClient, STACClient}

import java.net.URL
import java.time.{LocalDate, ZonedDateTime}
import scala.collection.{Map, mutable}

class OpenSearchClientTest {

  @Test
  def testOscarsGetProducts(): Unit = {
    val openSearch = OpenSearchClient(new URL("https://services.terrascope.be/catalogue"))

    val features = openSearch.getProducts(
      collectionId = "urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2",
      (LocalDate.of(2019, 10, 3), LocalDate.of(2020, 1, 2)),
      ProjectedExtent(Extent(2.688081576665092, 50.71625006623287, 5.838282906674661, 51.42339628212806), LatLng),
      Map[String, Any](), "hello", ""
    )

    println(s"got ${features.size} features")
    assertTrue(features.nonEmpty)
  }

  @Test
  def testCreoGetProducts(): Unit = {
    val openSearch = CreodiasClient

    val features = openSearch.getProducts(
      collectionId = "Sentinel2",
      (LocalDate.of(2020, 10, 1), LocalDate.of(2020, 10, 5)),
      ProjectedExtent(Extent(2.688081576665092, 50.71625006623287, 5.838282906674661, 51.42339628212806), LatLng),
      Map[String, Any](), correlationId = "hello", "LEVEL2A"
    )

    println(s"got ${features.size} features")
    assertTrue(features.nonEmpty)
    val filterFeatures = features.filter(_.id.contains("31UDT"))
    assertTrue(filterFeatures.nonEmpty)
    val aFeature = filterFeatures.head
    val band02 = aFeature.links.filter(_.title.get.contains("B02_10m"))
    assertTrue(band02.nonEmpty)
    assertEquals("31UDT",aFeature.tileID.get)
  }

  @Test
  def testSTACGetProducts(): Unit = {
    val openSearch = new STACClient()

    val features = openSearch.getProducts(
      collectionId = "sentinel-s2-l2a-cogs",
      (LocalDate.of(2020, 10, 1), LocalDate.of(2020, 10, 5)),
      ProjectedExtent(Extent(2.688081576665092, 50.71625006623287, 5.838282906674661, 51.42339628212806), LatLng),
      Map[String, Any](), correlationId = "hello", "LEVEL2A"
    )

    println(s"got ${features.size} features")
    assertTrue(features.nonEmpty)
    assertEquals(15,features.length)
  }

  /**
   * c-scale stac catalog is not stable
   */
  @Ignore
  @Test
  def testSTACGetProductsCScale(): Unit = {
    val openSearch = OpenSearchClient(new URL("https://resto.c-scale.zcu.cz/"))

    val features = openSearch.getProducts(
      collectionId = "S2",
      Option.empty,
      ProjectedExtent(Extent(-180, -90, 180, 90), LatLng),
      Map[String, Any](), correlationId = "hello", "LEVEL2A"
    )

    println(s"got ${features.size} features")
    assertTrue(features.nonEmpty)
  }

  @Test
  def testOscarsGetCollections(): Unit = {
    val client = OpenSearchClient(new URL("https://services.terrascope.be/catalogue"))
    checkGetCollections(client)
  }

  @Test
  def testCreoGetCollections(): Unit = {
    checkGetCollections(CreodiasClient)
  }

  @Test
  def testSTACGetCollections(): Unit = {
    checkGetCollections(new STACClient())
  }

  @Test
  def testSTACGetCollectionsCScale(): Unit = {
    checkGetCollections(new STACClient(new URL("https://resto.c-scale.zcu.cz/")))
  }

  private def checkGetCollections(openSearchClient: OpenSearchClient): Unit = {
    val features = openSearchClient.getCollections("hello")
    println(s"got ${features.size} features")
    val unique: mutable.Set[String] = mutable.Set()
    assertTrue(features.nonEmpty)
    features.foreach(f => {
      unique add f.id
    })
    assertEquals(features.size, unique.size)
  }


  @Test
  def testOscarsGetProductsWithEmptyDate(): Unit = {
    val openSearch = OpenSearchClient(new URL("https://services.terrascope.be/catalogue"))

    val features = openSearch.getProducts(
      collectionId = "urn:eop:VITO:COP_DEM_GLO_30M_COG",
      None,
      ProjectedExtent(Extent(2.688081576665092, 50.71625006623287, 5.838282906674661, 51.42339628212806), LatLng),
      Map[String, Any](), "hello", ""
    )

    println(s"got ${features.size} features")
    assertTrue(features.nonEmpty)
  }

  @Test
  def testCreoSentinel1(): Unit = {
    val openSearch = CreodiasClient

    val features = openSearch.getProducts(
      collectionId = "Sentinel1",
      (LocalDate.of(2020, 10, 1), LocalDate.of(2020, 10, 5)),
      ProjectedExtent(Extent(2.688081576665092, 50.71625006623287, 5.838282906674661, 51.42339628212806), LatLng),
      Map[String, Any](), "hello", ""
    )

    println(s"got ${features.size} features")
    val unique: mutable.Set[(Extent,ZonedDateTime)] = mutable.Set()
    assertTrue(features.nonEmpty)
    features.foreach(f => {
      assertTrue(f.id.contains("GRD"))
      val extent_timestamp = (f.bbox,f.nominalDate)
      unique add extent_timestamp
    })
    assertEquals(features.size,unique.size)
  }


}
