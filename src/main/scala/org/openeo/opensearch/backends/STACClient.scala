package org.openeo.opensearch.backends

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.opensearch.OpenSearchClient
import org.openeo.opensearch.OpenSearchResponses.{Feature, FeatureCollection, STACCollections, STACFeatureCollection}
import scalaj.http.HttpOptions

import java.net.URL
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.ISO_DATE_TIME
import scala.collection.Map

/**
 * SpatioTemporal Asset Catalog (STAC) client.
 * https://stacspec.org/en/about/stac-spec/
 *
 * @param endpoint The endpoint to retrieve the STAC features from.
 * @param s3URLS Whether the asset links in the STAC features should be converted to S3 bucket URLs.
 *               E.g. Conversion
 *               From: https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/SCL.tif
 *               To: s3://sentinel-cogs/sentinel-s2-l2a-cogs/SCL.tif
 */
class STACClient(private val endpoint: URL = new URL("https://earth-search.aws.element84.com/v0"),
                 private val s3URLS: Boolean = true) extends OpenSearchClient {

  require(endpoint != null)

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

    from(page = 1)
  }

  override protected def getProductsFromPage(collectionId: String,
                                             dateRange: Option[(ZonedDateTime, ZonedDateTime)],
                                             bbox: ProjectedExtent,
                                             attributeValues: Map[String, Any], correlationId: String,
                                             processingLevel: String, page: Int): FeatureCollection = {
    val Extent(xMin, yMin, xMax, yMax) = bbox.reproject(LatLng)
    var collectionsParam = collectionId
    var bboxParam = Array(xMin, yMin, xMax, yMax) mkString ","
    if (endpoint.getHost.contains("earth-search.aws.element84.com")) {
      collectionsParam = "[\"" + collectionId + "\"]"
      bboxParam = "[" + (Array(xMin, yMin, xMax, yMax) mkString ",") + "]"
    }

    var getProducts = http(s"$endpoint/search")
      .param("collections", collectionsParam)
      .param("limit", "100")
      .param("bbox", bboxParam)
      .param("page", page.toString)

    if (dateRange.isDefined) {
      getProducts = getProducts
        .param("datetime", dateRange.get._1.format(ISO_DATE_TIME) + "/" + dateRange.get._2.format(ISO_DATE_TIME))
    }

    val json = execute(getProducts)

    STACFeatureCollection.parse(json, toS3URL = s3URLS, dedup = true)
  }

  override def getCollections(correlationId: String = ""): Seq[Feature] = {
    val getCollections = http(s"$endpoint/collections")
      .option(HttpOptions.followRedirects(true))


    val json = execute(getCollections)
    
    STACCollections.parse(json).collections.map(c => Feature(c.id, null, null, null, null, None))
  }

  override final def equals(other: Any): Boolean = other match {
    case that: STACClient => this.endpoint == that.endpoint && this.s3URLS == that.s3URLS
    case _ => false
  }

  override final def hashCode(): Int = {
    val state = Seq(endpoint, s3URLS)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
