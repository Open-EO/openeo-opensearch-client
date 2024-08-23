package org.openeo.opensearch

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.openeo.TestHelpers.loadJsonResource
import org.openeo.opensearch.OpenSearchResponses.{CreoFeatureCollection, FeatureCollection}

import java.time.ZonedDateTime
import java.util.stream.{Stream => JStream}

object CreoFeatureCollectionTest {
  def tileIdPatterns: JStream[Arguments] = JStream.of(
    Arguments.of(None, Boolean.box(true)),
    Arguments.of(Some("31UFS"), Boolean.box(true)),
    Arguments.of(Some("30UFS"), Boolean.box(false)),
    Arguments.of(Some("31*"), Boolean.box(true)),
    Arguments.of(Some("30*"), Boolean.box(false)),
    Arguments.of(Some("31*F*"), Boolean.box(true)),
    Arguments.of(Some("30*F*"), Boolean.box(false)),
    Arguments.of(Some("3*U**"), Boolean.box(true)),
    Arguments.of(Some("3*T**"), Boolean.box(false)),
  )
}

class CreoFeatureCollectionTest {

  @ParameterizedTest
  @MethodSource(Array("tileIdPatterns"))
  def testTileIdPattern(tileIdPattern: Option[String], featureIncluded: Boolean): Unit = {
    val productsResponse = loadJsonResource("creoDiasTileIdPattern.json")
    val features = CreoFeatureCollection.parse(productsResponse, dedup = true, tileIdPattern = tileIdPattern).features

    assertEquals(featureIncluded, features.nonEmpty)
  }

  @Test
  def parseCreodiasDuppedFeature(): Unit = {
    val collectionsResponse = loadJsonResource("creodiasDuppedFeature.json")
    val features = CreoFeatureCollection.parse(collectionsResponse, dedup = true).features

    assertEquals(1, features.length)

    val feature = features(0)

    // Check if we really picked the latest Feature:
    assertEquals(ZonedDateTime.parse("2021-04-11T08:51:07.054814Z"), feature.generalProperties.published.get)
    assertEquals("/eodata/Sentinel-1/SAR/GRD/2021/04/11/S1B_IW_GRDH_1SDV_20210411T054146_20210411T054211_026415_032740_6184.SAFE", feature.id)
  }

  @Test
  def parseCreodiasSpecialDupped(): Unit = {
    val creodiasFeatureSnippet = loadJsonResource("creodiasFeatureSnippet.json")
    val composedJsonString =
      """{
        |  "type": "FeatureCollection",
        |  "properties": {
        |    "id": "fad6ca4a-3ab6-59e5-8e69-14aaf70256de",
        |    "totalResults": 2,
        |    "exactCount": true,
        |    "startIndex": 1,
        |    "itemsPerPage": 2,
        |    "links": [
        |      {
        |        "rel": "self",
        |        "type": "application/json",
        |        "title": "self",
        |        "href": "https://finder.creodias.eu/resto/api/collections/Sentinel1/search.json?&maxRecords=10&startDate=2021-04-11T00%3A00%3A00Z&completionDate=2021-04-11T23%3A59%3A59Z&productType=GRD&sensorMode=IW&geometry=POLYGON%28%285.785404630537803%2051.033953432779526%2C5.787426293119076%2051.021746940265956%2C5.803195261253003%2051.018694814851074%2C5.803195261253003%2051.02912208053834%2C5.785404630537803%2051.033953432779526%29%29&sortParam=startDate&sortOrder=descending&status=all&dataset=ESA-DATASET"
        |      },
        |      {
        |        "rel": "search",
        |        "type": "application/opensearchdescription+xml",
        |        "title": "OpenSearch Description Document",
        |        "href": "https://finder.creodias.eu/resto/api/collections/Sentinel1/describe.xml"
        |      }
        |    ]
        |  },
        |  "features": [""".stripMargin +
        creodiasFeatureSnippet.replaceAll("%startDate%", "2000-01-01T01:01:01.001Z") + ", \n" +
        creodiasFeatureSnippet.replaceAll("%startDate%", "2000-01-01T01:01:29.001Z") + ", \n" +
        creodiasFeatureSnippet.replaceAll("%startDate%", "2000-01-01T01:01:32.001Z") +
        "]}"
    val features = CreoFeatureCollection.parse(composedJsonString, dedup = true).features

    // Even tough the images are pairwise equal, the first and the last are not.
    // So the dedup code is forced to consider them as separate
    assertEquals(2, features.length)
  }

  @Test
  def parseCreodiasDifferentGeom(): Unit = {
    val collectionsResponse = loadJsonResource("creodiasDifferentGeom.json")
    val features = CreoFeatureCollection.parse(collectionsResponse, dedup = true).features

    assertEquals(3, features.length)
  }

  @Test
  def parseCreodiasPhoebus(): Unit = {
    val collectionsResponse = loadJsonResource("creodiasPhoebus.json")
    val features = CreoFeatureCollection.parse(collectionsResponse, dedup = true).features

    assertEquals(1, features.length)
  }

  @Test
  def parseCreodiasMergePhoebusFeatures(): Unit = {
    val collectionsResponse = loadJsonResource("creodiasMergePhoebusFeatures.json")
    val features = CreoFeatureCollection.parse(collectionsResponse, dedup = true).features

    assertEquals(1, features.length)
  }

  @Test
  def creodiasOffsetNeeded(): Unit = {
    val collectionsResponse = loadJsonResource("creodiasPixelValueOffsetNeeded.json")
    val features = CreoFeatureCollection.parse(collectionsResponse, dedup = true).features
    val link = features(0).links.find(l => l.title.get.contains("B04")).get
    assertEquals(-1000, link.pixelValueOffset.get, 1e-6)

    val linkSCL = features(0).links.find(l => l.title.get.contains("SCL")).get
    assertEquals(0, linkSCL.pixelValueOffset.get, 1e-6)
  }

  @Test
  def creodiasNoOffsetNeeded(): Unit = {
    val collectionsResponse = loadJsonResource("creodiasDifferentGeom.json")
    val features = CreoFeatureCollection.parse(collectionsResponse, dedup = true).features
    val link = features(0).links.find(l => l.title.get.contains("B04")).get
    assertEquals(0, link.pixelValueOffset.get, 1e-6)
  }

  @Test
  def testPagingReliesOnActualNumberOfFeaturesReturned(): Unit = {
    val productsResponse = loadJsonResource("creodiasUnreliablePagingValues.json")
    val FeatureCollection(itemsInPage, _) = CreoFeatureCollection.parse(productsResponse, dedup = true)

    assertEquals(4, itemsInPage)
  }

  @Test
  def sentinel1RTCResponse(): Unit = {
    val productsResponse = loadJsonResource("creodiasSentinel1RTCResponse.json")

    val FeatureCollection(_, features) = CreoFeatureCollection.parse(productsResponse, dedup = true)

    val expectedLinkTitles = Seq(
      "VV",
      "VH",
      "AREA",
      "MASK",
      "ANGLE"
    )
    features.foreach(f=>{
      assertTrue(expectedLinkTitles.forall(expectedTitle => f.links.exists(_.title contains expectedTitle)))
    })


  }

  @Test
  def landsat8L2Response(): Unit = {
    val productsResponse = loadJsonResource("creodiasLandsat8L2.json")

    val FeatureCollection(_, features) = CreoFeatureCollection.parse(productsResponse, dedup = true)
    val Array(feature) = features

    val expectedLinkTitles = Seq(
      "BAND_1",
      "BAND_2",
      "BAND_3",
      "BAND_4",
      "BAND_5",
      "BAND_6",
      "BAND_7",
      "BAND_ST_B10",
      "QUALITY_L1_PIXEL",
      "QUALITY_L1_RADIOMETRIC_SATURATION",
      "QUALITY_L2_AEROSOL",
      "QUALITY_L2_SURFACE_TEMPERATURE",
      "THERMAL_RADIANCE",
      "UPWELL_RADIANCE",
      "DOWNWELL_RADIANCE",
      "ATMOSPHERIC_TRANSMITTANCE",
      "EMISSIVITY",
      "EMISSIVITY_STDEV",
      "CLOUD_DISTANCE",
    )

    assertTrue(expectedLinkTitles.forall(expectedTitle => feature.links.exists(_.title contains expectedTitle)))
  }

  @Test
  def corruptTileIsOmitted(): Unit = {
    val productsResponse = loadJsonResource("creodiasCorruptTile.json")

    val corruptTileProductIdentifier =
      "/eodata/Sentinel-2/MSI/L2A_N0500/2018/03/27/S2A_MSIL2A_20180327T114351_N0500_R123_T29UMV_20230828T122340.SAFE"

    assertTrue(productsResponse contains corruptTileProductIdentifier,
      s"expected corrupt tile $corruptTileProductIdentifier in response")

    val FeatureCollection(_, features) = CreoFeatureCollection.parse(productsResponse, dedup = true)

    assertTrue(features.nonEmpty, "no features at all")
    val featureIds = features.map(_.id)

    assertTrue(featureIds.forall(_.startsWith("/eodata/Sentinel-2")), "unexpected product identifiers")
    assertFalse(featureIds contains corruptTileProductIdentifier, "corrupt tile is still present")
  }

  @Test
  def globalMosaicsSentinel1Response(): Unit = {
    val productsResponse = loadJsonResource("creodiasGlobalMosaicsSentinel1.json")

    val FeatureCollection(_, features) = CreoFeatureCollection.parse(productsResponse, dedup = true)

    val titledHrefs = for {
      feature <- features
      link <- feature.links
    } yield (link.title, link.href.toString)

    val expected = Set(
      (Some("VV"), "/eodata/Global-Mosaics/Sentinel-1/S1SAR_L3_IW_MCM/2023/12/01/Sentinel-1_IW_mosaic_2023_M12_50SMC_0_0/VV.tif"),
      (Some("VH"), "/eodata/Global-Mosaics/Sentinel-1/S1SAR_L3_IW_MCM/2023/12/01/Sentinel-1_IW_mosaic_2023_M12_50SMC_0_0/VH.tif"),
    )

    assertEquals(expected, titledHrefs.toSet)
  }
}
