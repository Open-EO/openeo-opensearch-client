package org.openeo.opensearch

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.openeo.TestHelpers.loadJsonResource
import org.openeo.opensearch.OpenSearchResponses.CreoFeatureCollection

import java.util.stream.{Stream => JStream}

object CreoFeatureCollectionTest {
  def tileIdPatterns: JStream[Arguments] = JStream.of(
    Arguments.of(None, Integer.valueOf(1)),
    Arguments.of(Some("31UFS"), Integer.valueOf(1)),
    Arguments.of(Some("30UFS"), Integer.valueOf(0)),
    Arguments.of(Some("31*"), Integer.valueOf(1)),
    Arguments.of(Some("30*"), Integer.valueOf(0)),
  )
}

class CreoFeatureCollectionTest {

  @ParameterizedTest
  @MethodSource(Array("tileIdPatterns"))
  def testTileIdPattern(tileIdPattern: Option[String], numFeaturesRemaining: Int): Unit = {
    val productsResponse = loadJsonResource("creoDiasTileIdPattern.json")
    val features = CreoFeatureCollection.parse(productsResponse, dedup = true, tileIdPattern = tileIdPattern).features

    assertEquals(numFeaturesRemaining, features.length)
  }
}
