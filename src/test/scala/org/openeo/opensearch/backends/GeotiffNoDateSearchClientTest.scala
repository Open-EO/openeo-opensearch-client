package org.openeo.opensearch.backends

import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.jupiter.api.Test

class GeotiffNoDateSearchClientTest {

  @Test
  def testSuitableAsCacheKey(): Unit = {
    EqualsVerifier.forClass(classOf[GeotiffNoDateSearchClient])
      .withNonnullFields("dataGlob", "bands", "defaultDate")
      .withIgnoredFields("pathsCache")
      .verify()
  }
}
