package org.openeo.opensearch.backends

import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.jupiter.api.Test

class GlobalNetCDFSearchClientTest {

  @Test
  def testSuitableAsCacheKey(): Unit =
    EqualsVerifier.forClass(classOf[GlobalNetCDFSearchClient])
      .withNonnullFields("dataGlob", "bands", "dateRegex", "gridExtent")
      .withIgnoredFields("pathsCache")
      .verify()
}
