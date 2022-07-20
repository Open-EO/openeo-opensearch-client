package org.openeo.opensearch.backends

import org.openeo.opensearch.OpenSearchResponses.{CreoCollections, CreoFeatureCollection, Feature, FeatureCollection}
import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.opensearch.OpenSearchClient
import scalaj.http.{Http, HttpOptions, HttpStatusException}

import java.net.URL
import java.time.{LocalDate, ZonedDateTime}
import java.time.format.DateTimeFormatter.ISO_INSTANT
import scala.collection.Map

class OscarsClient(val endpoint: URL) extends OpenSearchClient {

  def getStartAndEndDate(collectionId: String, attributeValues: Map[String, Any] = Map()): Option[(LocalDate, LocalDate)] = {
    def getFirstProductWithSortKey(key: String) = {
      val getProducts = Http(s"$endpoint/products")
        .param("collection", collectionId)
        .param("sortKeys", key)
        .param("count", "1")
        .params(attributeValues.mapValues(_.toString).toSeq)

      try  {
        val json = withRetries { execute(getProducts) }
        FeatureCollection.parse(json).features.headOption.map(_.nominalDate.toLocalDate)
      } catch {
        case _: HttpStatusException => Option.empty
      }
    }

    getFirstProductWithSortKey("start").flatMap(s => getFirstProductWithSortKey("start,,0").map(e => (s, e)))
  }

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
      if (itemsPerPage <= 0) Seq() else features ++ from(page + itemsPerPage)
    }

    from(page = 1)
  }

  override protected def getProductsFromPage(collectionId: String,
                                     dateRange: Option[(ZonedDateTime, ZonedDateTime)],
                                     bbox: ProjectedExtent,
                                     attributeValues: Map[String, Any], correlationId: String,
                                     processingLevel: String, page: Int): FeatureCollection = {
    val Extent(xMin, yMin, xMax, yMax) = bbox.reproject(LatLng)

    val newAttributeValues = collection.mutable.Map(attributeValues.toSeq: _*)
    newAttributeValues.getOrElseUpdate("accessedFrom", "MEP") // get direct access links instead of download urls

    var getProducts = http(s"$endpoint/products")
      .param("collection", collectionId)
      .param("bbox", Array(xMin, yMin, xMax, yMax) mkString ",")
      .param("sortKeys", "title") // paging requires deterministic order
      .param("startIndex", page.toString)
      .params(newAttributeValues.mapValues(_.toString).toSeq)
      .param("clientId", clientId(correlationId))

    if (dateRange.isDefined) {
      getProducts = getProducts
        .param("start", dateRange.get._1 format ISO_INSTANT)
        .param("end", dateRange.get._2 format ISO_INSTANT)
    }

    val json = withRetries { execute(getProducts) }
    FeatureCollection.parse(json)
  }

  override def getCollections(correlationId: String = ""): Seq[Feature] = {
    val getCollections = http(s"$endpoint/collections")
      .option(HttpOptions.followRedirects(true))
      .param("clientId", clientId(correlationId))

    val json = withRetries { execute(getCollections) }
    FeatureCollection.parse(json).features
  }

  override def equals(other: Any): Boolean = other match {
    case that: OscarsClient => this.endpoint == that.endpoint
    case _ => false
  }

  override def hashCode(): Int = endpoint.hashCode()
}
