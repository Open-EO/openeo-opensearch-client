package org.openeo

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.Ignore
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Disabled, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.openeo.opensearch.OpenSearchResponses.CreoFeatureCollection
import org.openeo.opensearch.backends.{CreodiasClient, STACClient}
import org.openeo.opensearch.{OpenSearchClient, ZonedDateTimeOrdering}

import java.net.URL
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util
import scala.collection.{Map, mutable}
import scala.io.Source
import scala.util.Using
import scala.xml.XML

object OpenSearchClientTest {
  def level1CParams: java.util.stream.Stream[Arguments] = util.Arrays.stream(Array(
    //    arguments(LocalDate.parse("2015-11-23"), new java.lang.Double(2.00)), // No products with this processingBaseline found
    arguments(LocalDate.parse("2015-12-23"), new java.lang.Double(2.01)), // tweaked date
    arguments(LocalDate.parse("2016-05-03"), new java.lang.Double(2.02)),
    //    arguments(LocalDate.parse("2016-06-09"), new java.lang.Double(2.03)), // No products with this processingBaseline found
    arguments(LocalDate.parse("2016-06-15"), new java.lang.Double(2.04)),
    arguments(LocalDate.parse("2017-05-03"), new java.lang.Double(2.05)), // tweaked date
    arguments(LocalDate.parse("2017-10-23"), new java.lang.Double(2.06)),
    arguments(LocalDate.parse("2018-11-06"), new java.lang.Double(2.07)),
    arguments(LocalDate.parse("2019-07-08"), new java.lang.Double(2.08)),
    arguments(LocalDate.parse("2020-02-04"), new java.lang.Double(2.09)),
    arguments(LocalDate.parse("2021-03-30"), new java.lang.Double(3.00)),
    arguments(LocalDate.parse("2021-06-30"), new java.lang.Double(3.01)),
    arguments(LocalDate.parse("2022-01-25"), new java.lang.Double(4.00)),
    arguments(LocalDate.parse("2022-12-06"), new java.lang.Double(5.09)),
    // No products with this processingBaseline 99.99 found for L2A
  ))

  def level2AParams: java.util.stream.Stream[Arguments] = util.Arrays.stream(Array(
    arguments(LocalDate.parse("2018-03-26"), new java.lang.Double(2.07)),
    //    arguments(LocalDate.parse("2018-05-23"), new java.lang.Double(2.08)), Has only PHOEBUS-core products
    arguments(LocalDate.parse("2018-10-08"), new java.lang.Double(2.09)),
    arguments(LocalDate.parse("2018-11-06"), new java.lang.Double(2.10)),
    arguments(LocalDate.parse("2018-11-21"), new java.lang.Double(2.11)),
    arguments(LocalDate.parse("2019-05-06"), new java.lang.Double(2.12)),
    arguments(LocalDate.parse("2019-07-08"), new java.lang.Double(2.13)),
    arguments(LocalDate.parse("2020-02-04"), new java.lang.Double(2.14)),
    arguments(LocalDate.parse("2021-03-30"), new java.lang.Double(3.00)),
    arguments(LocalDate.parse("2021-06-30"), new java.lang.Double(3.01)),
    arguments(LocalDate.parse("2022-01-25"), new java.lang.Double(4.00)),
    arguments(LocalDate.parse("2022-12-06"), new java.lang.Double(5.09)),
    arguments(LocalDate.parse("2018-08-31"), new java.lang.Double(99.99)), // Undocumented. Manually added
    arguments(LocalDate.parse("2021-10-19"), new java.lang.Double(5.0)), // Undocumented. Manually added
  ))

  def demExtents: java.util.stream.Stream[Arguments] = util.Arrays.stream(Array(
    arguments(ProjectedExtent(Extent(2.688081576665092, 50.71625006623287, 5.838282906674661, 51.42339628212806), LatLng)),
    arguments(ProjectedExtent(Extent(1.877486326846265, 50.00259421316291, 1.8797962194548734, 50.00408246028524), LatLng)), // from croptype
  ))
}

class OpenSearchClientTest {

  private var httpsCacheEnabledOriginalValue = false
  private var httpsCacherandomErrorEnabledOriginalValue = false

  @BeforeEach def beforeEach(): Unit = {
    httpsCacheEnabledOriginalValue = HttpCache.enabled
    httpsCacherandomErrorEnabledOriginalValue = HttpCache.randomErrorEnabled
  }

  @AfterEach def afterEach(): Unit = {
    HttpCache.enabled = httpsCacheEnabledOriginalValue
    HttpCache.randomErrorEnabled = httpsCacherandomErrorEnabledOriginalValue
  }

  @Test
  def testOscarsGetProducts(): Unit = {
    val openSearch = OpenSearchClient(new URL("https://services.terrascope.be/catalogue"))

    val endDate = LocalDate.of(2020, 1, 1)
    val features = openSearch.getProducts(
      collectionId = "urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2",
      (LocalDate.of(2019, 10, 3), endDate),
      ProjectedExtent(Extent(2.688081576665092, 50.71625006623287, 5.838282906674661, 51.42339628212806), LatLng),
      Map[String, Any]("eo:cloud_cover"->50.0,"resolution"->10), "hello", ""
      )

    println(s"got ${features.size} features")
    assertTrue(features.size<160)
    assertTrue(features.nonEmpty)
    val maxDate = features.map(_.nominalDate).max
    assertTrue(maxDate.isBefore(endDate.atStartOfDay(ZoneId.of("UTC"))))

  }

  @Test
  def testCreoGetProducts(): Unit = {
    val openSearch = CreodiasClient()

    val features = openSearch.getProducts(
      collectionId = "Sentinel2",
      (LocalDate.of(2020, 10, 1), LocalDate.of(2020, 10, 6)),
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
    assertEquals(1, features.length)
    assertEquals("/eodata/Sentinel-2/MSI/L2A/2018/08/12/S2A_MSIL2A_20180812T105621_N9999_R094_T30SVH_20220926T193221", features.head.id)
  }

  @Test
  def extentLatLngExtentToAtLeast1x1Test(): Unit = {
    assertEquals(Extent(0.0, 0.0, 1.0, 1.0), CreodiasClient.extentLatLngExtentToAtLeast1x1(Extent(0, 0, 0.001, 0.001)))
    assertEquals(Extent(179, 89, 180, 90), CreodiasClient.extentLatLngExtentToAtLeast1x1(Extent(180 - 0.001, 90 - 0.001, 180, 90)))
  }

  @ParameterizedTest
  @MethodSource(Array("demExtents"))
  def testCreoGetProductsDEM(extent: ProjectedExtent): Unit = {
    HttpCache.enabled = true
    val openSearch = new CreodiasClient()

    val features = openSearch.getProducts(
      collectionId = "CopDem",
      (LocalDate.of(2009, 10, 1), LocalDate.of(2020, 10, 5)),
      extent,
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
    assertEquals(11,features.length)
  }

  /**
   * c-scale stac catalog is not stable
   */
  @Ignore
  @Disabled
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
  @Disabled
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
      Map[String, Any]("productType"->"GRD"), "hello", "LEVEL1"
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

  @ParameterizedTest
  @MethodSource(Array("level1CParams"))
  def testManifestLevelSentinel2_L1C(date: LocalDate, processingBaseline: java.lang.Double): Unit = {
    // Cache reduces test time from 5min to 1sec.
    HttpCache.enabled = true
    // Bands found with JSONPath: $..[?(@.id=="SENTINEL2_L1C")]..["eo:bands"][?(@.aliases)].aliases
    val requiredBands = Set(
      "IMG_DATA_Band_60m_1_Tile1_Data",
      "IMG_DATA_Band_10m_1_Tile1_Data",
      "IMG_DATA_Band_10m_2_Tile1_Data",
      "IMG_DATA_Band_10m_3_Tile1_Data",
      "IMG_DATA_Band_20m_1_Tile1_Data",
      "IMG_DATA_Band_20m_2_Tile1_Data",
      "IMG_DATA_Band_20m_3_Tile1_Data",
      "IMG_DATA_Band_10m_4_Tile1_Data",
      "IMG_DATA_Band_20m_4_Tile1_Data",
      "IMG_DATA_Band_60m_2_Tile1_Data",
      "IMG_DATA_Band_60m_3_Tile1_Data",
      "IMG_DATA_Band_20m_5_Tile1_Data",
      "IMG_DATA_Band_20m_6_Tile1_Data",
      "IMG_DATA_Band_TCI_Tile1_Data",
      "S2_Level-1C_Tile1_Metadata",
      "S2_Level-1C_Product_Metadata",
      // Emile: Specific bands don't seem fully supported yet
      // "S2_Level-1C_Tile1_Metadata##0",
      // "S2_Level-1C_Tile1_Metadata##1",
      // "S2_Level-1C_Tile1_Metadata##2",
      // "S2_Level-1C_Tile1_Metadata##3",
    )
    val selectedFeature = testManifestLevelSentinel2(date, processingBaseline, "L1C", "S2MSI1C", requiredBands)

    // Testing special link to bands that contain sun and view angle information:
    val metadataBand = selectedFeature.links.find(_.href.toString.endsWith("MTD_TL.xml")).get
    if (metadataBand.href.toString.contains("/PHOEBUS-core/")) {
      throw new Exception("There should be no PHOEBUS-core in results.")
    } else {
      val str = {
        val in = Source.fromInputStream(CreoFeatureCollection.loadMetadata(metadataBand.href.toString))
        try in.mkString.trim
        finally in.close()
      }
      assertTrue(str.contains("Tile_Angles")) // small sanity check. Angle bands are not fully supported yet.
      XML.loadString(str) // Test if XML is parsable
    }
  }

  @ParameterizedTest
  @MethodSource(Array("level2AParams"))
  def testManifestLevelSentinel2_L2A(date: LocalDate, processingBaseline: java.lang.Double): Unit = {
    // Cache reduces test time from 3min to 2sec.
    HttpCache.enabled = true
    HttpCache.randomErrorEnabled = false // To test retry
    // Bands found with JSONPath: $..[?(@.id=="SENTINEL2_L2A")]..["eo:bands"][?(@.aliases)].aliases
    val requiredBands = Set(
      "IMG_DATA_Band_B01_60m_Tile1_Data",
      "IMG_DATA_Band_B02_10m_Tile1_Data",
      "IMG_DATA_Band_B03_10m_Tile1_Data",
      "IMG_DATA_Band_B04_10m_Tile1_Data",
      "IMG_DATA_Band_B05_20m_Tile1_Data",
      "IMG_DATA_Band_B06_20m_Tile1_Data",
      "IMG_DATA_Band_B07_20m_Tile1_Data",
      "IMG_DATA_Band_B08_10m_Tile1_Data",
      "IMG_DATA_Band_B8A_20m_Tile1_Data",
      "IMG_DATA_Band_B09_60m_Tile1_Data",
      "IMG_DATA_Band_B11_20m_Tile1_Data",
      "IMG_DATA_Band_B12_20m_Tile1_Data",
      "IMG_DATA_Band_TCI_10m_Tile1_Data",
      "IMG_DATA_Band_WVP_10m_Tile1_Data",
      "IMG_DATA_Band_AOT_20m_Tile1_Data",
      "IMG_DATA_Band_SCL_20m_Tile1_Data",
      "S2_Level-2A_Tile1_Metadata",
      "S2_Level-2A_Product_Metadata",
    )
    val selectedFeature = testManifestLevelSentinel2(date, processingBaseline, "L2A", "S2MSI2A", requiredBands)

    // Testing special link to bands that contain sun and view angle information:
    val metadataBand = selectedFeature.links.find(_.href.toString.endsWith("MTD_TL.xml")).get
    if (metadataBand.href.toString.contains("/PHOEBUS-core/")) {
      throw new Exception("There should be no PHOEBUS-core in results.")
    } else {
      val str = {
        val in = Source.fromInputStream(CreoFeatureCollection.loadMetadata(metadataBand.href.toString))
        try in.mkString.trim
        finally in.close()
      }
      assertTrue(str.contains("Tile_Angles")) // small sanity check. Angle bands are not fully supported yet.
      XML.loadString(str) // Test if XML is parsable
    }
  }

  private def testManifestLevelSentinel2(date: LocalDate,
                                         processingBaseline: Double,
                                         productType: String,
                                         processingLevel: String,
                                         requiredBands: Set[String]
                                        ) = {
    /*
    // Run this snippet in the JS console to extract boilerplate code from the page
    // https://sentinels.copernicus.eu/web/sentinel/technical-guides/sentinel-2-msi/processing-baseline
    const tables = document.querySelectorAll(".sentinel-table")
    let str = ""
    for(t of tables){
        const name = t.querySelector("caption").innerText.replaceAll("-", "_").replaceAll(" ", "_")
        //str += `val ${name} = Map(\n`
        str += `def ${name}: java.util.stream.Stream[Arguments] = util.Arrays.stream(Array(\n`
        const rows = t.querySelectorAll("tr:not(.tableheader)")
        for(row of rows){
          const tds = row.querySelectorAll("td")
            const processingVersion = parseFloat(tds[0].innerText)
            let date = new Date(tds[1].innerText)
            date=new Date(date.getTime() - date.getTimezoneOffset() * 60000)
            date = date.toISOString().split("T")[0]
            //str += `    LocalDate.parse("${date}") -> ${processingVersion.toFixed(2)},\n`
            str += `    arguments(LocalDate.parse("${date}"), new java.lang.Double(${processingVersion.toFixed(2)})),\n`

        }
        str += `))\n`
    }
    console.log(str)
     */

    val extentTAP4326 = Extent(5.07, 51.215, 5.08, 51.22)
    val features = new CreodiasClient().getProducts(
      collectionId = "Sentinel2",
      Some(Tuple2(date.atStartOfDay(UTC), date.plusDays(6).atStartOfDay(UTC))),
      ProjectedExtent(extentTAP4326, LatLng),
      Map(
        "productType" -> productType,
        "processingBaseline" -> processingBaseline, // avoid old products getting dedupped away
      ),
      correlationId = "hello",
      processingLevel,
    )

    for (feature <- features) {
      // Checking bands for all features:
      val foundBands = feature.links.map(_.title.get).toSet
        .filter(l => !l.contains("Mask_") && !l.contains("_InformationData")) // filter for nicer debugging
      val intersect = requiredBands.intersect(foundBands)
      assertEquals(requiredBands.mkString("\n"), intersect.mkString("\n"))
    }
    // Generally, each date a product baseline is released, there will a be a product with that baseline in the first few days.
    // This is the product we are actually interested in.
    features.find(_.generalProperties.processingBaseline.get == processingBaseline).get
  }

  @Test
  /**
   * Links refering to PHOEBUS-core should be ignored: https://github.com/Open-EO/openeo-opensearch-client/issues/16
   */
  def parseCreodiasCorruptPhoebus(): Unit = {
    HttpCache.enabled = true
    val url = "https://finder.creodias.eu/oldresto/api/collections/Sentinel2/search.json?box=21.657597756412194%2C46.02854700799339%2C21.95285234099209%2C46.23461502351761&sortParam=startDate&sortOrder=ascending&page=1&maxRecords=100&status=0%7C34%7C37&dataset=ESA-DATASET&productType=L2A&cloudCover=%5B0%2C95%5D&startDate=2018-08-20T00%3A00%3A00Z&completionDate=2018-08-20T23%3A59%3A59.999999999Z"
    val collectionsResponse = Using(Source.fromURL(new URL(url))) { source => source.getLines.mkString("\n") }.get
    val features = CreoFeatureCollection.parse(collectionsResponse, dedup = true).features

    for {
      f <- features
      l <- f.links
      if l.href.toString.contains("/PHOEBUS-core/")
    } {
      // there used to be a product with processingBaseline == 2.08
      throw new Exception("There should be no PHOEBUS-core in results.")
    }
    assertEquals(1, features.length)
  }

  @Test
  def nonNodedIntersection(): Unit = {
    HttpCache.enabled = true
    val url = "https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel2/search.json?box=-3.4850284734588555%2C42.557489967174575%2C-3.204481304802883%2C42.7667871856319&sortParam=startDate&sortOrder=ascending&page=2&maxRecords=100&status=ONLINE&dataset=ESA-DATASET&productType=L2A&cloudCover=%5B0%2C95%5D&startDate=2021-05-09T00%3A00%3A00Z&completionDate=2021-10-11T23%3A59%3A59.999999999Z"
    val collectionsResponse = Using(Source.fromURL(new URL(url))) { source => source.getLines.mkString("\n") }.get
    val features = CreoFeatureCollection.parse(collectionsResponse, dedup = true).features

    // TODO
    assertEquals(6, features.length)
  }
}
