package org.openeo.opensearch.backends

import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.jupiter.api.Test

class OscarsClientTest {

  @Test
  def testSuitableAsCacheKey(): Unit =
    EqualsVerifier.forClass(classOf[OscarsClient])
      .withNonnullFields("endpoint")
      .verify()
}
