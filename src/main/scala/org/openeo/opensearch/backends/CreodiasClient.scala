package org.openeo.opensearch.backends

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.opensearch.{OpenSearchClient, safeReproject, to_0_360_range}
import org.openeo.opensearch.OpenSearchResponses.{CreoCollections, CreoFeatureCollection, Feature, FeatureCollection}
import org.slf4j.LoggerFactory
import scalaj.http.HttpOptions

import java.net.URL
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.ISO_INSTANT
import scala.collection.Map

object CreodiasClient{
  private val logger = LoggerFactory.getLogger(classOf[CreodiasClient])
  private val pageSize = 1000

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

class CreodiasClient(val endpoint: URL = new URL("https://catalogue.dataspace.copernicus.eu/resto"),
                     val allowParallelQuery: Boolean = false, val oneOrbitPerDay: Boolean = false) extends OpenSearchClient {
  import CreodiasClient._

  require(endpoint != null)

  private def collection(collectionId: String) = s"${endpoint.toString}/api/collections/$collectionId/search.json"

  override def getProducts(collectionId: String,
                           dateRange: Option[(ZonedDateTime, ZonedDateTime)],
                           bbox: ProjectedExtent,
                           attributeValues: Map[String, Any], correlationId: String,
                           processingLevel: String): Seq[Feature] = {
    if (allowParallelQuery && dateRange.isDefined) {
      // DEM has a big difference between beginDate and completionDate.
      // The temporal extent should span both dates, so we avoid doing this for DEM.
      // For Sentinel2, beginDate and completionDate look alike so this is no issue here.
      def from(startDate: ZonedDateTime): Seq[(ZonedDateTime, ZonedDateTime)] = {
        var endDate = startDate.plusYears(1)
        // begin < product < end. Products that fall exactly on beginning or end are excluded
        if (endDate.isAfter(dateRange.get._2)) {
          endDate = dateRange.get._2
        }
        val features = (startDate, endDate.plusNanos(1))
        if (endDate == dateRange.get._2 || endDate.isAfter(dateRange.get._2)) {
          Seq(features)
        } else {
          Seq(features) ++ from(startDate.plusYears(1))
        }
      }

      from(dateRange.get._1).par.flatMap(range => {
        getProductsSplitAntimeridian(collectionId,
          Some(range), bbox,
          attributeValues, correlationId,
          processingLevel
        )
      }).seq
    } else {
      getProductsSplitAntimeridian(collectionId, dateRange, bbox, attributeValues, correlationId, processingLevel)
    }
  }

  def getProductsSplitAntimeridian(collectionId: String,
                                   dateRange: Option[(ZonedDateTime, ZonedDateTime)],
                                   bbox: ProjectedExtent,
                                   attributeValues: Map[String, Any], correlationId: String,
                                   processingLevel: String
                                  ): Seq[Feature] = {
    val bboxProjected = safeReproject(bbox, LatLng)
    var products = getProductsOriginal(collectionId, dateRange, bboxProjected, attributeValues, correlationId, processingLevel)

    if (bboxProjected.extent.xmax >= 180) {
      // Products in catalog range seems to span -182 to +182
      val e = bboxProjected.extent
      val swapped = Extent(to_0_360_range(e.xmin) - 360, e.ymin, to_0_360_range(e.xmax) - 360, e.ymax)
      val bboxProjectedNegative = ProjectedExtent(swapped, bboxProjected.crs)
      products ++= getProductsOriginal(collectionId, dateRange, bboxProjectedNegative, attributeValues, correlationId, processingLevel)
    }

    products
  }

  def getProductsOriginal(collectionId: String,
                          dateRange: Option[(ZonedDateTime, ZonedDateTime)],
                          bbox: ProjectedExtent,
                          attributeValues: Map[String, Any], correlationId: String,
                          processingLevel: String): Seq[Feature] = {
    // First try fast unsorted query. If there are too many results, try again, with sorted pagination
    val collection = getProductsFromPageCustom(collectionId, dateRange, bbox, attributeValues,
      correlationId, processingLevel, 1, sorted = false)
    if (collection.features.length < pageSize * 0.9) {
      // 90% threshold, just in case. Probably not needed
      collection.features
    } else {
      logger.info("Too many features to download in one request. Using pagination. correlationId: " + correlationId)

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
  }

  override protected def getProductsFromPage(collectionId: String,
                                             dateRange: Option[(ZonedDateTime, ZonedDateTime)],
                                             bbox: ProjectedExtent,
                                             attributeValues: Map[String, Any], correlationId: String,
                                             processingLevel: String, page: Int): FeatureCollection = {
    getProductsFromPageCustom(collectionId, dateRange, bbox, attributeValues,
      correlationId, processingLevel, page, sorted = true)
  }

  protected def getProductsFromPageCustom(collectionId: String,
                                          dateRange: Option[(ZonedDateTime, ZonedDateTime)],
                                          bbox: ProjectedExtent,
                                          attributeValues: Map[String, Any], correlationId: String,
                                          processingLevel: String, page: Int,
                                          sorted: Boolean): FeatureCollection = {
    var bboxReprojected = safeReproject(bbox, LatLng).extent
    if (attributeValues.get("resolution").contains(30) && attributeValues.get("productType").contains("DGE_30")) {
      // Otherwise catalogue.dataspace.copernicus.eu might return a 504 error
      bboxReprojected = extentLatLngExtentToAtLeast1x1(bboxReprojected)
    }
    val Extent(xMin, yMin, xMax, yMax) = bboxReprojected

    var getProducts = http(collection(collectionId))
      .param("box", Array(xMin, yMin, xMax, yMax) mkString ",")
      .param("page", page.toString)
      .param("maxRecords", pageSize.toString) // A larger page size does not slow down the requests.
      .param("status", "ONLINE")
      .param("dataset", "ESA-DATASET")
      .params(attributeValues.mapValues(_.toString).filterKeys(isPropagated).toSeq)

    if (sorted) {
      getProducts = getProducts
        .param("sortParam", "startDate") // paging requires deterministic order
        .param("sortOrder", "ascending")
    } else if (page != 1) {
      throw new IllegalArgumentException("When sorted == false, it is undeterministic to request later pages")
    }

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

    val relativeOrbit = attributeValues.get("sat:relative_orbit")
    if(relativeOrbit.isDefined) {
      getProducts = getProducts.param("relativeOrbitNumber",s"${relativeOrbit.get.toString}")
    }

    if (dateRange.isDefined) {
      getProducts = getProducts
        .param("startDate", dateRange.get._1 format ISO_INSTANT)
        .param("completionDate", dateRange.get._2 format ISO_INSTANT)
    }

    val tileIdValue = attributeValues.get("tileId")

    tileIdValue match {
      case Some(pattern: String) if !pattern.contains("*") =>
        getProducts = getProducts.param("tileId", pattern)
        logger.debug(s"included non-wildcard tileId $pattern param")
      case _ =>
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
    CreoFeatureCollection.parse(json, dedup = true, tileIdValue, oneOrbitPerDay)
  }

  private def isPropagated(attribute: String): Boolean =
    !Set("eo:cloud_cover", "provider:backend", "orbitDirection", "sat:orbit_state", "processingBaseline", "tileId", "sat:relative_orbit")
      .contains(attribute)

  override def getCollections(correlationId: String): Seq[Feature] = {
    val getCollections = http(s"$endpoint/collections.json")
      .option(HttpOptions.followRedirects(true))

    val json = execute(getCollections)
    CreoCollections.parse(json).collections.map(c => Feature(c.name, null, null, null, null,None))
  }


  override final def equals(other: Any): Boolean = other match {
    case that: CreodiasClient => (
      this.endpoint == that.endpoint
        && this.allowParallelQuery == that.allowParallelQuery
        && this.oneOrbitPerDay == that.oneOrbitPerDay
      )
    case _ => false
  }

  override final def hashCode(): Int = {
    val state = Seq(endpoint, allowParallelQuery, oneOrbitPerDay)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
