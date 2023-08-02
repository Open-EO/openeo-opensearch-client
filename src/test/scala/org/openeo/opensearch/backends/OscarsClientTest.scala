package org.openeo.opensearch.backends

import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.Test

import java.net.URL
import scala.collection.immutable.HashMap

class OscarsClientTest {

  @Test
  def testSuitableAsCacheKey(): Unit = {
    val cache = HashMap(new OscarsClient(new URL("https://oscars.example.org"), isUTM = true) -> 5)

    assertTrue(cache contains new OscarsClient(new URL("https://oscars.example.org"), isUTM = true))
    assertFalse(cache contains new OscarsClient(new URL("https://oscars.example.org"), isUTM = false))
  }
}
