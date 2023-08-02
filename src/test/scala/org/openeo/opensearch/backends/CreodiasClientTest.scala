package org.openeo.opensearch.backends

import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.jupiter.api.Test

class CreodiasClientTest {

  @Test
  def testSuitableAsCacheKey(): Unit = {
    EqualsVerifier.forClass(classOf[CreodiasClient])
      .withNonnullFields("endpoint")
      .verify()
  }
}
