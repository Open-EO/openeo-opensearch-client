package org.openeo.opensearch.backends

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.opensearch.OpenSearchClient
import org.openeo.opensearch.OpenSearchResponses.{CreoCollections, CreoFeatureCollection, Feature, FeatureCollection}
import scalaj.http.HttpOptions

import java.net.URL
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.ISO_INSTANT
import scala.collection.Map

object CreodiasClient{

  def apply(): CreodiasClient = {new CreodiasClient()}

  def extentLatLngExtentToAtLeast1x1(extent: Extent): Extent = {
    val worldExtent = Extent(-180.0, -90.0, 180.0, 90.0)
    var extentReturn = extent
    extentReturn = extentReturn
      .copy(xmax = Math.min(Math.max(extentReturn.xmin + 1, extentReturn.xmax), worldExtent.xmax))
      .copy(ymax = Math.min(Math.max(extentReturn.ymin + 1, extentReturn.ymax), worldExtent.ymax))

    // For max touched a world border, try extending the min
    extentReturn = extentReturn
      .copy(xmin = Math.max(Math.min(extentReturn.xmax - 1, extentReturn.xmin), worldExtent.xmin))
      .copy(ymin = Math.max(Math.min(extentReturn.ymax - 1, extentReturn.ymin), worldExtent.ymin))
    extentReturn
  }
}

class CreodiasClient(val endpoint: URL = new URL("https://catalogue.dataspace.copernicus.eu/resto")) extends OpenSearchClient {
  import CreodiasClient._

  private val collections = s"${endpoint.toString}/collections.json"
  private def collection(collectionId: String) = s"${endpoint.toString}/api/collections/$collectionId/search.json"

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
    var bboxReprojected = bbox.reproject(LatLng)
    if (attributeValues.get("resolution").contains(30) && attributeValues.get("productType").contains("DGE_30")) {
      // Otherwise catalogue.dataspace.copernicus.eu might return a 504 error
      bboxReprojected = extentLatLngExtentToAtLeast1x1(bboxReprojected)
    }
    val Extent(xMin, yMin, xMax, yMax) = bboxReprojected

    var getProducts = http(collection(collectionId))
      .param("box", Array(xMin, yMin, xMax, yMax) mkString ",")
      .param("sortParam", "startDate") // paging requires deterministic order
      .param("sortOrder", "ascending")
      .param("page", page.toString)
      .param("maxRecords", "100")
      .param("status", "ONLINE")
      .param("dataset", "ESA-DATASET")
      .params(attributeValues.mapValues(_.toString).filterKeys(!Seq( "eo:cloud_cover", "provider:backend", "orbitDirection", "sat:orbit_state", "processingBaseline").contains(_)).toSeq)

    val cloudCover = attributeValues.get("eo:cloud_cover")
    if(cloudCover.isDefined) {
      getProducts = getProducts.param("cloudCover",s"[0,${cloudCover.get.toString.toDouble.toInt}]")
    }

    if(!processingLevel.isEmpty) {
      getProducts = getProducts.param("processingLevel", processingLevel)
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
    }

    /*
      // HACK: Putting pb as latest filter changes request time from 1.5min to 20sec.
      // Used this JS snippet to debug it:
      let urlSlow = new URL("https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel2/search.json?box=5.069685009564397%2C51.21481793134414%2C5.08036486094712%2C51.22021524164683&sortParam=startDate&sortOrder=ascending&page=1&maxRecords=100&status=0%7C34%7C37&dataset=ESA-DATASET&processingBaseline=2.07&productType=L1C&startDate=2018-11-06T00%3A00%3A00Z&completionDate=2018-11-12T23%3A59%3A59.999999999Z")
      //let url = window.location
      let str = `val request = http("${url.toString().replace(url.search, "")}")\n`
      url.searchParams.forEach((value,key)=>str+=`  .param("${key}", "${value}")\n`)
      console.log(str)
     */
    val processingBaseline = attributeValues.get("processingBaseline")
    if (processingBaseline.isDefined) {
      getProducts = getProducts.param("processingBaseline", processingBaseline.get.toString)
    }

    val json = execute(getProducts)
    CreoFeatureCollection.parse(json, dedup = true)
  }

  override def getCollections(correlationId: String): Seq[Feature] = {
    val getCollections = http(collections)
      .option(HttpOptions.followRedirects(true))

    val json = execute(getCollections)
    CreoCollections.parse(json).collections.map(c => Feature(c.name, null, null, null, null,None))
  }
}
