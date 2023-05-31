package org.openeo

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.Assert._
import org.junit.{Ignore, Test}
import org.openeo.opensearch.{OpenSearchClient, OpenSearchResponses}
import org.openeo.opensearch.backends.{CreodiasClient, STACClient}
import scalaj.http.HttpRequest

import java.net.URL
import java.nio.file.{Files, Path, Paths}
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}
import java.util
import scala.collection.{Map, mutable}
import scala.io.{Codec, Source}

class OpenSearchClientTest {

  @Test
  def testOscarsGetProducts(): Unit = {
    val openSearch = OpenSearchClient(new URL("https://services.terrascope.be/catalogue"))

    val features = openSearch.getProducts(
      collectionId = "urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2",
      (LocalDate.of(2019, 10, 3), LocalDate.of(2020, 1, 2)),
      ProjectedExtent(Extent(2.688081576665092, 50.71625006623287, 5.838282906674661, 51.42339628212806), LatLng),
      Map[String, Any]("eo:cloud_cover"->50.0,"resolution"->10), "hello", ""
      )

    println(s"got ${features.size} features")
    assertTrue(features.size<160)
    assertTrue(features.nonEmpty)
  }

  @Test
  def testCreoGetProducts(): Unit = {
    val openSearch = CreodiasClient()

    val features = openSearch.getProducts(
      collectionId = "Sentinel2",
      (LocalDate.of(2020, 10, 1), LocalDate.of(2020, 10, 5)),
      ProjectedExtent(Extent(2.688081576665092, 50.71625006623287, 5.838282906674661, 51.42339628212806), LatLng),
      Map[String, Any]("eo:cloud_cover"->90.0), correlationId = "hello", "S2MSI2A"
      )

    println(s"got ${features.size} features")
    assertTrue(features.nonEmpty)
    assertTrue(features.size<9)
    val filterFeatures = features.filter(_.id.contains("31UDT"))
    assertTrue(filterFeatures.nonEmpty)
    val aFeature = filterFeatures.head
    val band02 = aFeature.links.filter(_.title.get.contains("B02_10m"))
    assertTrue(band02.nonEmpty)
    assertEquals("31UDT",aFeature.tileID.get)
    assertTrue(aFeature.geometry.isDefined)
  }

  @Test
  def testIgnoreZeroResolution(): Unit = {
    val openSearch = CreodiasClient()

    val features = openSearch.getProducts(
      collectionId = "Sentinel2",
      (LocalDate.of(2018, 8, 12), LocalDate.of(2018, 8, 13)),
      ProjectedExtent(Extent(-3.7937789378418247, 38.486414764328515, -3.5314443712734733, 38.69684729114566), LatLng),
      Map[String, Any]("eo:cloud_cover" -> 90.0, "productType" -> "L2A"), correlationId = "hello", "S2MSI2A"
    )
    assertEquals(2, features.length)
    assertEquals("/eodata/Sentinel-2/MSI/L2A/2018/08/12/S2A_MSIL2A_20180812T105621_N0213_R094_T30SVH_20201026T165913.SAFE", features.head.id)
  }

  @Test
  def testCreoGetProductsDEM(): Unit = {
    val openSearch = new CreodiasClient()

    val features = openSearch.getProducts(
      collectionId = "CopDem",
      (LocalDate.of(2009, 10, 1), LocalDate.of(2020, 10, 5)),
      ProjectedExtent(Extent(2.688081576665092, 50.71625006623287, 5.838282906674661, 51.42339628212806), LatLng),
      Map[String, Any]("productType"->"DGE_30", "resolution"->30), correlationId = "hello", ""
    )

    println(s"got ${features.size} features")
    assertTrue(features.nonEmpty)
    assertTrue(features.size<11)

    val aFeature = features.head
    val band02 = aFeature.links.filter(_.title.get.contains("DEM"))
    assertTrue(band02.nonEmpty)
    val location = band02(0).href.toString
    assertTrue(location.endsWith("DEM.tif"))

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

  @Ignore // Old Finder API (RESTO) will be deprecated in March 2023.
  @Test
  def testCreoGetCollections(): Unit = {
    checkGetCollections(new CreodiasClient())
  }

  @Test
  def testSTACGetCollections(): Unit = {
    checkGetCollections(new STACClient())
  }

  @Ignore
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
    val openSearch = new CreodiasClient()

    val features = openSearch.getProducts(
      collectionId = "Sentinel1",
      (LocalDate.of(2020, 10, 1), LocalDate.of(2020, 10, 5)),
      ProjectedExtent(Extent(2.688081576665092, 50.71625006623287, 5.838282906674661, 51.42339628212806), LatLng),
      Map[String, Any](), "hello", "LEVEL1"
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

  @Test
  def testManifestLevelSentinel2_L1C(): Unit = {
    val dates1C = Map(
//      LocalDate.parse("2015-11-23") -> 2.00, // No found
      LocalDate.parse("2015-12-15") -> 2.01,
      LocalDate.parse("2016-05-03") -> 2.02,
//      LocalDate.parse("2016-06-09") -> 2.03,
      LocalDate.parse("2016-06-15") -> 2.04,
      LocalDate.parse("2017-04-27") -> 2.05,
      LocalDate.parse("2017-10-23") -> 2.06,
      LocalDate.parse("2018-11-06") -> 2.07,
      LocalDate.parse("2019-07-08") -> 2.08,
      LocalDate.parse("2020-02-04") -> 2.09,
      LocalDate.parse("2021-03-30") -> 3.00,
      LocalDate.parse("2021-06-30") -> 3.01,
      LocalDate.parse("2022-01-25") -> 4.00,
      LocalDate.parse("2022-12-06") -> 5.09,
    )
    val requiredBands = Set(
      "IMG_DATA_Band_60m_1_Tile1_Data",
      "IMG_DATA_Band_10m_1_Tile1_Data",
      "IMG_DATA_Band_10m_2_Tile1_Data",
      "IMG_DATA_Band_10m_3_Tile1_Data",
      "IMG_DATA_Band_20m_1_Tile1_Data",
      "IMG_DATA_Band_20m_2_Tile1_Data",
      "IMG_DATA_Band_20m_3_Tile1_Data",
      "IMG_DATA_Band_10m_4_Tile1_Data",
      "IMG_DATA_Band_60m_2_Tile1_Data",
      "IMG_DATA_Band_60m_3_Tile1_Data",
      "IMG_DATA_Band_20m_5_Tile1_Data",
      "IMG_DATA_Band_20m_6_Tile1_Data",
      "IMG_DATA_Band_20m_4_Tile1_Data",
    )
    testManifestLevelSentinel2(dates1C, "L1C", "S2MSI1C", requiredBands)
  }

  @Test
  def testManifestLevelSentinel2_L2A(): Unit = {
    val Level_2A = Map(
      LocalDate.parse("2018-03-26") -> 2.07,
      LocalDate.parse("2018-05-23") -> 2.08,
      LocalDate.parse("2018-10-08") -> 2.09,
      LocalDate.parse("2018-11-06") -> 2.10,
      LocalDate.parse("2018-11-21") -> 2.11,
      LocalDate.parse("2019-05-06") -> 2.12,
      LocalDate.parse("2019-07-08") -> 2.13,
      LocalDate.parse("2020-02-04") -> 2.14,
      LocalDate.parse("2021-03-30") -> 3.00,
      LocalDate.parse("2021-06-30") -> 3.01,
      LocalDate.parse("2022-01-25") -> 4.00,
      LocalDate.parse("2022-12-06") -> 5.09,
    )
    val requiredBands = Set(
      "IMG_DATA_Band_AOT_20m_Tile1_Data",
      "IMG_DATA_Band_B01_60m_Tile1_Data",
      "IMG_DATA_Band_B02_10m_Tile1_Data",
      "IMG_DATA_Band_B03_10m_Tile1_Data",
      "IMG_DATA_Band_B04_10m_Tile1_Data",
      "IMG_DATA_Band_B05_20m_Tile1_Data",
      "IMG_DATA_Band_B06_20m_Tile1_Data",
      "IMG_DATA_Band_B07_20m_Tile1_Data",
      "IMG_DATA_Band_B08_10m_Tile1_Data",
      "IMG_DATA_Band_B09_60m_Tile1_Data",
      "IMG_DATA_Band_B11_20m_Tile1_Data",
      "IMG_DATA_Band_B12_20m_Tile1_Data",
      "IMG_DATA_Band_B8A_20m_Tile1_Data",
      "IMG_DATA_Band_SCL_20m_Tile1_Data",
      "IMG_DATA_Band_TCI_10m_Tile1_Data",
      "IMG_DATA_Band_WVP_10m_Tile1_Data",
    )
    testManifestLevelSentinel2(Level_2A, "L2A", "S2MSI2A", requiredBands)
  }

  def testManifestLevelSentinel2(datesToBaselineMap: Map[LocalDate, Double], productType: String, processingLevel: String, requiredBands: Set[String]): Unit = {
    /*
    // Run this snippet in the JS console to extract boilerplate code from the page
    // https://sentinels.copernicus.eu/web/sentinel/technical-guides/sentinel-2-msi/processing-baseline
    const tables = document.querySelectorAll(".sentinel-table")
    let str = ""
    for(t of tables){
        const name = t.querySelector("caption").innerText.replaceAll("-", "_").replaceAll(" ", "_")
        str += `val ${name} = Map(\n`
        const rows = t.querySelectorAll("tr:not(.tableheader)")
        for(row of rows){
          const tds = row.querySelectorAll("td")
            const processingVersion = parseFloat(tds[0].innerText)
            let date = new Date(tds[1].innerText)
            date=new Date(date.getTime() - date.getTimezoneOffset() * 60000)
            date = date.toISOString().split("T")[0]
            str += `    LocalDate.parse("${date}") -> ${processingVersion.toFixed(2)},\n`
        }
        str += `)\n`
    }
    console.log(str)
     */

    val openSearchCached = new CreodiasClient() {
      override def execute(request: HttpRequest): String = {
        val fullUrl = request.urlBuilder(request)
        val idx = fullUrl.indexOf("//")
        var filePath = fullUrl.substring(idx + 2)
        val lastSlash = filePath.lastIndexOf("/")
        var (basePath, filename) = filePath.splitAt(lastSlash + 1)
        if (filename.length > 255) {
          filename = (filename.hashCode >>> 1).toString + ".json" // TODO, parse extension
        }
        filePath = basePath + filename

        val cachePath = "tmp/" // TODO: move to resources
        val path = Paths.get(cachePath + filePath)
        if (!Files.exists(path)) {
          Files.createDirectories(Paths.get(cachePath + basePath))
          println("Need to download first: " + path)
          Files.write(path, super.execute(request).getBytes)
        } else {
          println("Using download mock: " + path)
        }
        val jsonFile = Source.fromFile(path.toString)(Codec.UTF8)

        try jsonFile.mkString
        finally jsonFile.close()
      }

      override def getProductsFromPage(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: Map[String, Any], correlationId: String, processingLevel: String, page: Int): OpenSearchResponses.FeatureCollection =
        super.getProductsFromPage(collectionId, dateRange, bbox, attributeValues, correlationId, processingLevel, page)
    }

    val extentTAP4326 = Extent(5.07, 51.215, 5.08, 51.22)
    var donePbs = Set[Double]()
    var requiredProcesingBaselines = datesToBaselineMap.values.toSet
//    requiredProcesingBaselines += 99.99 // This value is not documented, but best to test it
    for {
      (date, _) <- datesToBaselineMap
      features = openSearchCached.getProducts(
        collectionId = "Sentinel2",
        Some(Tuple2(date.atStartOfDay(UTC), date.plusDays(40).atStartOfDay(UTC))), // Big time frame to avoid product gap in 2016
        ProjectedExtent(extentTAP4326, LatLng),
        Map("productType" -> productType),
        correlationId = "hello",
        processingLevel,
      )
      pb <- requiredProcesingBaselines.diff(donePbs)
      feature <- features.find(_.generalProperties.processingBaseline.get == pb)
    } {
      // Generally, each date a product baseline is released, there will a be a product with that baseline in the first 40 days.
      // But we interpret is flexible, because it might change. And this way, we can also test the undocumented 99.99
      donePbs += pb
      val foundBands = feature.links.map(_.title.get).toSet
      val intersect = requiredBands.intersect(foundBands)
      assertEquals(requiredBands, intersect)
    }
    assertEquals(requiredProcesingBaselines, donePbs.intersect(requiredProcesingBaselines))
  }
}
