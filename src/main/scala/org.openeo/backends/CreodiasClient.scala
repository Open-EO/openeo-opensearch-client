package org.openeo.backends

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.ISO_INSTANT

import org.openeo.OpenSearchClient
import org.openeo.OpenSearchResponses.{CreoCollections, CreoFeatureCollection, Feature, FeatureCollection}
import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import scalaj.http.HttpOptions

import scala.collection.Map

object CreodiasClient extends OpenSearchClient {
  private val collections = "https://finder.creodias.eu/resto/collections.json"
  private def collection(collectionId: String) = s"https://finder.creodias.eu/resto/api/collections/$collectionId/search.json"

  override def getProducts(collectionId: String,
                           dateRange: Option[(ZonedDateTime, ZonedDateTime)],
                           bbox: ProjectedExtent,
                           attributeValues: Map[String, Any], correlationId: String,
                           processingLevel: String): Seq[Feature] = {
    def from(page: Int): Seq[Feature] = {
      val FeatureCollection(itemsPerPage, features) = getProductsFromPage(collectionId,
        dateRange, bbox,
        attributeValues, correlationId,
        processingLevel,
        page)
      if (itemsPerPage <= 0) Seq() else features ++ from(page + 1)
    }

    from(1)
  }

  override protected def getProductsFromPage(collectionId: String,
                                             dateRange: Option[(ZonedDateTime, ZonedDateTime)],
                                             bbox: ProjectedExtent,
                                             attributeValues: Map[String, Any], correlationId: String,
                                             processingLevel: String, page: Int): FeatureCollection = {
    val Extent(xMin, yMin, xMax, yMax) = bbox.reproject(LatLng)

    var getProducts = http(collection(collectionId))
      .param("processingLevel", processingLevel)
      .param("box", Array(xMin, yMin, xMax, yMax) mkString ",")
      .param("sortParam", "startDate") // paging requires deterministic order
      .param("sortOrder", "ascending")
      .param("page", page.toString)
      .param("maxRecords", "100")
      .param("status", "0|34|37")
      .param("dataset", "ESA-DATASET")
      .params(attributeValues.mapValues(_.toString).toSeq)

    if (dateRange.isDefined) {
      getProducts = getProducts
        .param("startDate", dateRange.get._1 format ISO_INSTANT)
        .param("completionDate", dateRange.get._2 format ISO_INSTANT)
    }

    if( "Sentinel1".equals(collectionId)) {
      getProducts = getProducts.param("timeliness","Fast-24h").param("productType","GRD")
    }

    val json = withRetries { execute(getProducts) }
    CreoFeatureCollection.parse(json)
  }

  override def getCollections(correlationId: String): Seq[Feature] = {
    val getCollections = http(collections)
      .option(HttpOptions.followRedirects(true))

    val json = withRetries { execute(getCollections) }
    CreoCollections.parse(json).collections.map(c => Feature(c.name, null, null, null, null,None))
  }
}
