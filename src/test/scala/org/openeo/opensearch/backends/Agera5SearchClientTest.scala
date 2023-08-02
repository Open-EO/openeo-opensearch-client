package org.openeo.opensearch.backends

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

class Agera5SearchClientTest {

  @Test
  def testSuitableAsCacheKey(): Unit = {
    val a = new Agera5SearchClient("/data/MTDA/AgERA5/2*/*/AgERA5_dewpoint-temperature_*.tif",
      bands = Seq("dewpoint-temperature").asJava, dateRegex = ".+_(\\d{4})(\\d{2})(\\d{2})\\.tif".r)

    val b = new Agera5SearchClient("/data/MTDA/AgERA5/2*/*/AgERA5_dewpoint-temperature_*.tif",
      bands = Seq("dewpoint-temperature").asJava, dateRegex = ".+_(\\d{4})(\\d{2})(\\d{2})\\.tif".r)

    assertEquals(a, b)

    val cache = HashMap(a -> 5)

    assertTrue(cache contains b)

    assertFalse(cache contains new Agera5SearchClient("/data/MTDA/AgERA5/2*/*/AgERA5_dewpoint-temperature_*.tif",
      bands = Seq("precipitation-flux").asJava, dateRegex = ".+_(\\d{4})(\\d{2})(\\d{2})\\.tif".r))
  }
}
