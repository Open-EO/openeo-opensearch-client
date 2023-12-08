package org.openeo.opensearch.backends

import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.openeo.opensearch.OpenSearchClient

import java.net.URL

class OscarsClientTest {

  @Test
  def testSuitableAsCacheKey(): Unit =
    EqualsVerifier.forClass(classOf[OscarsClient])
      .withNonnullFields("endpoint")
      .verify()

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
