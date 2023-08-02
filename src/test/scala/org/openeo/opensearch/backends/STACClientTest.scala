package org.openeo.opensearch.backends

import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.jupiter.api.Test

class STACClientTest {

  @Test
  def testSuitableAsCacheKey(): Unit =
    EqualsVerifier.forClass(classOf[STACClient])
      .withNonnullFields("endpoint")
      .verify()
}
