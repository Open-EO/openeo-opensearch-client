package org.openeo

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.Ignore
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.openeo.opensearch.OpenSearchResponses.CreoFeatureCollection
import org.openeo.opensearch.{OpenSearchClient, OpenSearchResponses}
import org.openeo.opensearch.backends.{CreodiasClient, STACClient}

import java.net.URL
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}
import java.util
import scala.collection.{Map, mutable}
import scala.io.Source
import scala.xml.XML

object OpenSearchClientTest {
  def level1CParams: java.util.stream.Stream[Arguments] = util.Arrays.stream(Array(
//    arguments(LocalDate.parse("2015-11-23"), new java.lang.Double(2.00)), // No products with this processingBaseline found
    arguments(LocalDate.parse("2015-12-15"), new java.lang.Double(2.01)),
    arguments(LocalDate.parse("2016-05-03"), new java.lang.Double(2.02)),
//    arguments(LocalDate.parse("2016-06-09"), new java.lang.Double(2.03)), // No products with this processingBaseline found
    arguments(LocalDate.parse("2016-06-15"), new java.lang.Double(2.04)),
    arguments(LocalDate.parse("2017-04-27"), new java.lang.Double(2.05)),
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
    arguments(LocalDate.parse("2018-05-23"), new java.lang.Double(2.08)),
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
  ))

  class HttpsCache extends sun.net.www.protocol.https.Handler {

    import java.io.{File, FileInputStream, InputStream}
    import java.net.URL
    import java.nio.file.{Files, Paths}

    var enabled = false

    def openConnectionSuper(url: URL): java.net.URLConnection = super.openConnection(url)

    override def openConnection(url: URL): java.net.URLConnection = new java.net.HttpURLConnection(url) {
      private lazy val inputStream = {
        if (!enabled) {
          openConnectionSuper(url).getInputStream
        } else {
          val fullUrl = this.url.toString
          val idx = fullUrl.indexOf("//")
          var filePath = fullUrl.substring(idx + 2)
          if (filePath.length > 255) {
            // An individual name should be max 255 characters long. Lazy implementation caps whole file path:
            val hash = "___" + (filePath.hashCode >>> 1).toString // TODO, parse extension?
            filePath = filePath.substring(0, 255 - hash.length) + hash
          }
          val lastSlash = filePath.lastIndexOf("/")
          val (basePath, filename) = filePath.splitAt(lastSlash + 1)
          filePath = basePath + filename

           val cachePath = getClass.getResource("/org/openeo/httpsCache").getPath
//          val cachePath = "src/test/resources/org/openeo/httpsCache"
          val path = Paths.get(cachePath, filePath)
          if (!Files.exists(path)) {
            println("Caching request url: " + url)
            Files.createDirectories(Paths.get(cachePath, basePath))
            val stream = openConnectionSuper(url).getInputStream
            val tmpBeforeAtomicMove = Paths.get(cachePath, java.util.UUID.randomUUID().toString)
            // TODO: Test for binary images.
            Files.write(tmpBeforeAtomicMove, scala.io.Source.fromInputStream(stream).mkString.getBytes)
            Files.move(tmpBeforeAtomicMove, path)
          } else {
            println("Using cached request: " + path.toUri)
          }
          new FileInputStream(new File(path.toString))
        }
      }

      override def getInputStream: InputStream = inputStream

      override def connect(): Unit = {}

      override def disconnect(): Unit = ???

      override def usingProxy(): Boolean = ???
    }
  }

  val httpsCache = new HttpsCache()
  // This method can be called at most once in a given Java Virtual Machine:
  java.net.URL.setURLStreamHandlerFactory(new java.net.URLStreamHandlerFactory() {
    override def createURLStreamHandler(protocol: String): java.net.URLStreamHandler =
      if (protocol == "http" || protocol == "https") httpsCache else null
  })
}

class OpenSearchClientTest {
  import OpenSearchClientTest._

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
    assertEquals(1, features.length)
    assertEquals("/eodata/Sentinel-2/MSI/L2A/2018/08/12/S2A_MSIL2A_20180812T105621_N9999_R094_T30SVH_20220926T193221", features.head.id)
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

  @ParameterizedTest
  @MethodSource(Array("level1CParams"))
  def testManifestLevelSentinel2_L1C(date: LocalDate, processingBaseline: java.lang.Double): Unit = {
    // Cache reduces test time from 5min to 1sec.
    val httpsCacheEnabledOriginalValue = httpsCache.enabled
    httpsCache.enabled = true
    try {
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
        // Emile: Specific bands don't seem fully supported yet
        // "S2_Level-1C_Tile1_Metadata##0",
        // "S2_Level-1C_Tile1_Metadata##1",
        // "S2_Level-1C_Tile1_Metadata##2",
        // "S2_Level-1C_Tile1_Metadata##3",
      )
      val selectedFeature = testManifestLevelSentinel2(date, processingBaseline, "L1C", "S2MSI1C", requiredBands)

      // Testing special link to bands that contain suna and view angle information:
      val metadataBand = selectedFeature.links.find(_.href.toString.endsWith("MTD_TL.xml")).get
      if (metadataBand.href.toString.contains("/PHOEBUS-core/") && processingBaseline == 2.08) {
        println("Ignorind old product")
      } else {
        val str = {
          val in = Source.fromInputStream(CreoFeatureCollection.loadMetadata(metadataBand.href.toString))
          try in.mkString.trim
          finally in.close()
        }
        assertTrue(str.contains("Tile_Angles")) // small sanity check. Angle bands are not fullty supported yet.
        XML.loadString(str) // Test if XML is parsable
      }
    } finally {
      httpsCache.enabled = httpsCacheEnabledOriginalValue
    }
  }

  @ParameterizedTest
  @MethodSource(Array("level2AParams"))
  def testManifestLevelSentinel2_L2A(date: LocalDate, processingBaseline: java.lang.Double): Unit = {
    // Cache reduces test time from 3min to 2sec.
    val httpsCacheEnabledOriginalValue = httpsCache.enabled
    httpsCache.enabled = true
    try {
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
        "S2_Level-2A_Product_Metadata",
      )
      val selectedFeature = testManifestLevelSentinel2(date, processingBaseline, "L2A", "S2MSI2A", requiredBands)

      // Testing special link to bands that contain suna and view angle information:
      val metadataBand = selectedFeature.links.find(_.href.toString.endsWith("MTD_TL.xml")).get
      if (metadataBand.href.toString.contains("/PHOEBUS-core/") && processingBaseline == 2.08) {
        println("Ignoring old product")
      } else {
        val str = {
          val in = Source.fromInputStream(CreoFeatureCollection.loadMetadata(metadataBand.href.toString))
          try in.mkString.trim
          finally in.close()
        }
        assertTrue(str.contains("Tile_Angles")) // small sanity check. Angle bands are not fullty supported yet.
        XML.loadString(str) // Test if XML is parsable
      }
    } finally {
      httpsCache.enabled = httpsCacheEnabledOriginalValue
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
      Some(Tuple2(date.atStartOfDay(UTC), date.plusDays(15).atStartOfDay(UTC))),
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
      val intersect = requiredBands.intersect(foundBands)
      assertEquals(requiredBands, intersect)
    }
    // Generally, each date a product baseline is released, there will a be a product with that baseline in the first few days.
    // This is the product we are actually interested in.
    features.find(_.generalProperties.processingBaseline.get == processingBaseline).get
  }
}
