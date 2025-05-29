package org.openeo.opensearch.backends

import geotrellis.vector.ProjectedExtent
import org.openeo.opensearch.OpenSearchResponses.{Feature, FeatureCollection}
import org.openeo.opensearch.{OpenSearchClient, OpenSearchResponses}

import java.time.ZonedDateTime

object MockOpenSearchClient {

  def apply(featuresString: String, isUTM: Boolean = true, dedup: Boolean = false, deduplicationPropertyJsonPath: String = "properties.published"): MockOpenSearchClient = {
    val features = FeatureCollection.parse(featuresString, isUTM = isUTM, dedup = dedup, deduplicationPropertyJsonPath = deduplicationPropertyJsonPath).features
    new MockOpenSearchClient(features)
  }

  def apply(features: FeatureCollection): MockOpenSearchClient = {
    new MockOpenSearchClient(features.features)
  }
}

class MockOpenSearchClient(val features: Seq[Feature]) extends OpenSearchClient {

  override def getProducts(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String): Seq[OpenSearchResponses.Feature] = {
    features
  }

  override protected def getProductsFromPage(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String, startIndex: Int): OpenSearchResponses.FeatureCollection = ???

  override def getCollections(correlationId: String): Seq[OpenSearchResponses.Feature] = ???

  override def equals(that: Any): Boolean = this eq that.asInstanceOf[AnyRef]

  override def hashCode(): Int = System.identityHashCode(this)
}