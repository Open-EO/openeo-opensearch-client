package org.openeo.opensearch.backends

import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.jupiter.api.Test

class Agera5SearchClientTest {

  @Test
  def testSuitableAsCacheKey(): Unit =
    EqualsVerifier.forClass(classOf[Agera5SearchClient])
      .withNonnullFields("dataGlob", "bands", "dateRegex", "bandMarker")
      .withIgnoredFields("pathsCache")
      .verify()
}
