package org.openeo.opensearch.backends

import org.openeo.opensearch.OpenSearchClient
import org.openeo.opensearch.OpenSearchResponses.{Feature, FeatureCollection, STACCollections, STACFeatureCollection}
import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}

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

    val (collectionsParam, bboxParam) = if (endpoint.getHost.contains("earth-search.aws.element84.com"))
        ("[\"" + collectionId + "\"]", "[" + (Array(xMin, yMin, xMax, yMax) mkString ",") + "]")
      else
        (collectionId, Array(xMin, yMin, xMax, yMax) mkString ",")

    val getProducts = http(s"$endpoint/search")
      .param("collections", collectionsParam)
      .param("limit", "100")
      .param("bbox", bboxParam)
      .param("page", page.toString)

    val getProductsForDateRange = dateRange.foldLeft(getProducts) { case (req, (fromDate, toDate)) =>
      req.param("datetime", s"${fromDate format ISO_DATE_TIME}/${toDate format ISO_DATE_TIME}")
    }

    val json = withRetries {
      execute(getProductsForDateRange)
    }

    STACFeatureCollection.parse(json, toS3URL = s3URLS, dedup = true)
  }

  override def getCollections(correlationId: String = ""): Seq[Feature] = {
    val getCollections = http(s"$endpoint/collections")

    val json = withRetries {
      execute(getCollections)
    }
    STACCollections.parse(json).collections.map(c => Feature(c.id, null, null, null, null, None))
  }

  override def equals(other: Any): Boolean = other match {
    case that: STACClient => this.endpoint == that.endpoint && this.s3URLS == that.s3URLS
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(endpoint, s3URLS)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
