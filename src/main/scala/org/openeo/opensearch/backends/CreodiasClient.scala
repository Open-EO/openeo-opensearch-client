package org.openeo.opensearch.backends

import java.time.{ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter.ISO_INSTANT
import org.openeo.opensearch.OpenSearchResponses.{CreoCollections, CreoFeatureCollection, Feature, FeatureCollection, GeneralProperties}
import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.opensearch.OpenSearchClient
import scalaj.http.HttpOptions

import scala.collection.Map

object CreodiasClient extends OpenSearchClient {
  private val collections = "https://finder.creodias.eu/resto/collections.json"
  private def collection(collectionId: String) = s"https://finder.creodias.eu/resto/api/collections/$collectionId/search.json"
  private val sentinel1_switch_date = ZonedDateTime.of(2021,2,23,0,0,0,0,ZoneId.of("UTC"))

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
      .params(attributeValues.mapValues(_.toString).filterKeys(!Seq( "eo:cloud_cover", "provider:backend", "orbitDirection", "sat:orbit_state").contains(_)).toSeq)

    val cloudCover = attributeValues.get("eo:cloud_cover")
    if(cloudCover.isDefined) {
      getProducts = getProducts.param("cloudCover",s"[0,${cloudCover.get.toString.toDouble.toInt}]")
    }

    val orbitdirection = attributeValues.get("orbitDirection").orElse(attributeValues.get("sat:orbit_state"))
    if(orbitdirection.isDefined) {
      getProducts = getProducts.param("orbitDirection",orbitdirection.get.toString.toLowerCase)
    }

    if (dateRange.isDefined) {
      getProducts = getProducts
        .param("startDate", dateRange.get._1 format ISO_INSTANT)
        .param("completionDate", dateRange.get._2 format ISO_INSTANT)
    }

    if( "Sentinel1".equals(collectionId)) {
      getProducts = getProducts.param("productType","GRD")
      if(dateRange.isDefined && !attributeValues.contains("timeliness")) {
        //ESA decided to redefine the meaning of the timeliness property at some point
        //https://sentinels.copernicus.eu/web/sentinel/-/copernicus-sentinel-1-nrt-3h-and-fast24h-products
        if(dateRange.get._1.isBefore(sentinel1_switch_date)) {
          getProducts = getProducts.param("timeliness","Fast-24h")
        }else{
          getProducts = getProducts.param("timeliness","NRT-3h|Fast-24h")
        }

      }
    }

    val json = withRetries { execute(getProducts) }
    CreoFeatureCollection.parse(json)
  }

  override def getCollections(correlationId: String): Seq[Feature] = {
    val getCollections = http(collections)
      .option(HttpOptions.followRedirects(true))

    val json = withRetries { execute(getCollections) }
    CreoCollections.parse(json).collections.map(c => Feature(c.name, null, null, null, null, new GeneralProperties(),None))
  }
}
