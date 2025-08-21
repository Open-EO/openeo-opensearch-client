package org.openeo

import geotrellis.raster.geotiff.GeoTiffRasterSource
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.io.{File, InputStream}
import java.net.URL
import scala.io.Source
import scala.language.postfixOps

object HttpCacheTest {
  def sourceToBytes(source: InputStream): Array[Byte] = {
    LazyList.continually(source.read()).takeWhile(-1 !=).map(_.toByte).toArray
  }
}

class HttpCacheTest {

  import HttpCacheTest._

  private var httpsCacheEnabledOriginalValue = false

  @BeforeEach def beforeEach(): Unit = {
    httpsCacheEnabledOriginalValue = HttpCache.enabled
  }

  @AfterEach def afterEach(): Unit = {
    HttpCache.enabled = httpsCacheEnabledOriginalValue
  }

  @Test
  def testTime(): Unit = {
    HttpCache.enabled = true
    val url = "https://zipper.creodias.eu/get-object?path=/Sentinel-2/MSI/L2A/2018/03/27/S2B_MSIL2A_20180327T105019_N0207_R051_T31UFS_20180327T134916.SAFE/manifest.safe"

    val sourceWarmup = Source.fromURL(new URL(url))
    sourceWarmup.getLines.mkString("\n")

    val t0 = System.currentTimeMillis()
    val source = Source.fromURL(new URL(url))
    val content = source.getLines.mkString("\n")
    source.close()
    val t1 = System.currentTimeMillis()
    val difference = t1 - t0
    println("time difference: " + difference)
    assertTrue(difference < 30) // generally it takes 2ms, but taking some margin
    assertTrue(content.startsWith("<?xml"))
  }

  @Test
  def testJson(): Unit = {
    HttpCache.enabled = true
    val url = "https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel2/search.json?box=5.07%2C51.215%2C5.08%2C51.22&sortParam=startDate&sortOrder=ascending&page=1&maxRecords=100&productType=L2A&startDate=2018-03-26T00%3A00%3A00Z&completionDate=2018-04-10T00%3A00%3A00Z"

    val sourceWarmup = Source.fromURL(new URL(url))
    sourceWarmup.getLines.mkString("\n")

    val t0 = System.currentTimeMillis()
    val source = Source.fromURL(new URL(url))
    val content = source.getLines.mkString("\n")
    source.close()
    val t1 = System.currentTimeMillis()
    val difference = t1 - t0
    println("time difference: " + difference)
    assertTrue(difference < 30) // generally it takes 2ms, but taking some margin
    assertTrue(content.startsWith("{\""))
  }

  @Test
  def testImage(): Unit = {
    HttpCache.enabled = true
    val url = "https://openeo.org/images/openeo_logo.png"

    val sourceWarmup = new URL(url).openStream()
    val contentWarmup = sourceToBytes(sourceWarmup)
    assertEquals(-119, contentWarmup(0))
    assertEquals(80, contentWarmup(1))
    assertEquals(78, contentWarmup(2))

    val t0 = System.currentTimeMillis()
    val source = new URL(url).openStream()
    val content = sourceToBytes(source)
    source.close()
    val t1 = System.currentTimeMillis()
    val difference = t1 - t0
    println("time difference: " + difference)
    assertTrue(difference < 30) // generally it takes 2ms, but taking some margin
    assertEquals(-119, content(0))
    assertEquals(80, content(1))
    assertEquals(78, content(2))
  }

  @Test
  def testParallel(): Unit = {
    HttpCache.enabled = true
    val url = "https://openeo.org/images/openeo_navbar_logo.png"

    val cachePath = new File("""/\D:/""".r.replaceAllIn(getClass.getResource("/org/openeo/httpsCache/").getPath + "openeo.org/images/openeo_navbar_logo.png", "/"))
    cachePath.delete()

    Seq(url, url).foreach(url => {
      val source = new URL(url).openStream()
      val content = sourceToBytes(source)
      assertEquals(-119, content(0))
    })
    assertTrue(cachePath.exists())
  }

  @Test
  def testRasterSource(): Unit = {
    HttpCache.enabled = true // Tiff not fully supported, but should not give errors anyway
    val t0 = System.currentTimeMillis()

    val b04RasterSource = GeoTiffRasterSource("https://artifactory.vgt.vito.be/artifactory/testdata-public/S2_B04_timeseries.tiff")
    b04RasterSource.read().get

    val t1 = System.currentTimeMillis()
    val difference = t1 - t0
    println("time difference: " + difference) // >2000ms
  }
}
