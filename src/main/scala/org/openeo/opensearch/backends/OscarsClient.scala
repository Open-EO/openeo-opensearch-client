package org.openeo.opensearch.backends

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.opensearch.OpenSearchClient
import org.openeo.opensearch.OpenSearchResponses.{Feature, FeatureCollection, dedupFeatures}
import scalaj.http.{HttpOptions, HttpStatusException}

import java.net.URL
import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.time.format.DateTimeFormatter.ISO_INSTANT
import java.time.{LocalDate, ZonedDateTime}
import java.util.Locale
import scala.collection.Map

/**
 *
 * @param endpoint
 * @param isUTM
 * @param deduplicationPropertyJsonPath When removing duplicates, the code will keep products where this property has the highest value.
 */
class OscarsClient(val endpoint: URL, val isUTM: Boolean = false, val deduplicationPropertyJsonPath: String = "properties.published") extends OpenSearchClient {
  require(endpoint != null)
  require(deduplicationPropertyJsonPath != null)

  def getStartAndEndDate(collectionId: String, attributeValues: Map[String, Any] = Map()): Option[(LocalDate, LocalDate)] = {
    def getFirstProductWithSortKey(key: String) = {
      val getProducts = http(s"$endpoint/products")
        .param("collection", collectionId)
        .param("sortKeys", key)
        .param("count", "1")
        .params(attributeValues.mapValues(_.toString).toSeq)

      try  {
        val json = execute(getProducts)
        FeatureCollection.parse(json).features.headOption.map(_.nominalDate.toLocalDate)
      } catch {
        case _: HttpStatusException => Option.empty
      }
    }

    for {
      startDate <- getFirstProductWithSortKey("start")
      endDate <- getFirstProductWithSortKey("start,,0")
    } yield (startDate, endDate)
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

    val features = from(page = 1)
    // In case features on different pages would match.
    // This was not an issue, but better safe than sorry.
    // The pages get also dedupped individually, so this is just a final cleanup.
    dedupFeatures(features.toArray)
  }

  override protected def getProductsFromPage(collectionId: String,
                                     dateRange: Option[(ZonedDateTime, ZonedDateTime)],
                                     bbox: ProjectedExtent,
                                     attributeValues: Map[String, Any], correlationId: String,
                                     processingLevel: String, page: Int): FeatureCollection = {
    val Extent(xMin, yMin, xMax, yMax) = bbox.reproject(LatLng)

    val propagatableAttributeValues =
      (if (attributeValues.contains("accessedFrom")) attributeValues else attributeValues + ("accessedFrom" -> "MEP")) // get direct access links instead of download urls
        .filter {
          case ("tileId", value) => value.isInstanceOf[String] // filter by single tileId (server side)
          case (attribute, _) => !(Seq("eo:cloud_cover", "provider:backend", "orbitDirection", "sat:orbit_state", "sat:relative_orbit")
            contains attribute)
        }

    val coordinateFormat = new DecimalFormat("0.#######", DecimalFormatSymbols.getInstance(Locale.ROOT))

    var getProducts = http(s"$endpoint/products")
      .param("collection", collectionId)
      .param("bbox", Array(xMin, yMin, xMax, yMax).map(coordinateFormat.format) mkString ",")
      .param("sortKeys", "title") // paging requires deterministic order
      .param("startIndex", page.toString)
      .params(propagatableAttributeValues.mapValues(_.toString).toMap)
      .param("clientId", clientId(correlationId))

    val cloudCover = attributeValues.get("eo:cloud_cover")
    if(cloudCover.isDefined) {
      getProducts = getProducts.param("cloudCover",s"[0,${cloudCover.get.toString.toDouble.toInt}]")
    }

    val relativeOrbit = attributeValues.get("sat:relative_orbit")
    if(relativeOrbit.isDefined) {
      getProducts = getProducts.param("relativeOrbitNumber",s"${relativeOrbit.get.toString}")
    }

    val orbitdirection = attributeValues.get("orbitDirection").orElse(attributeValues.get("sat:orbit_state"))
    if (orbitdirection.isDefined) {
      getProducts = getProducts.param("orbitDirection", orbitdirection.get.toString.toUpperCase)
    }

    if (dateRange.isDefined) {
      getProducts = getProducts
        .param("start", dateRange.get._1 format ISO_INSTANT)
        .param("end", dateRange.get._2 format ISO_INSTANT)
    }

    val json = execute(getProducts)

    val resultCollection = FeatureCollection.parse(
      json,
      isUTM,
      dedup = true,
      deduplicationPropertyJsonPath = deduplicationPropertyJsonPath
    )

    filterByDateRange(
      filterByTileIds(resultCollection, attributeValues.get("tileId")), dateRange)
  }

  private def filterByDateRange(featureCollection: FeatureCollection,
                                dateRangeValue: Option[(ZonedDateTime, ZonedDateTime)]): FeatureCollection = {
    //oscars actually manages to return features that are outside of the daterange for coherence
    dateRangeValue match {
      case Some((from, until)) => featureCollection.copy(features = featureCollection.features.filter { feature =>
        feature.nominalDate.isEqual(from) || (feature.nominalDate.isAfter(from) &&
          (feature.nominalDate.isBefore(until) || (from isEqual until)))
      })
      case _ => featureCollection
    }
  }

  private def filterByTileIds(featureCollection: FeatureCollection, tileIdValue: Option[Any]): FeatureCollection = {
    // filter by multiple tileIds (client side)
    tileIdValue match {
      case Some(tileIds: java.util.List[String]) => featureCollection.copy(
        features = featureCollection.features.filter { feature =>
          feature.tileID match {
            case Some(tileId) => tileIds contains tileId
            case _ => false
          }
        })
      case _ => featureCollection
    }
  }

  override def getCollections(correlationId: String = ""): Seq[Feature] = {
    def from(startIndex: Int): Seq[Feature] = {
      val features = getCollectionsFromPage(
        correlationId,
        startIndex,
      )
      if (features.length <= 0) Seq() else features ++ from(startIndex + features.length)
    }

    from(1)
  }

  private def getCollectionsFromPage(correlationId: String = "", startIndex: Int): Seq[Feature] = {
    val getCollections = http(s"$endpoint/collections")
      .option(HttpOptions.followRedirects(true))
      .param("count", "200")
      .param("startIndex", startIndex.toString)
      .param("clientId", clientId(correlationId))

    val json = execute(getCollections)
    FeatureCollection.parse(json, dedup=false).features
  }

  override final def equals(other: Any): Boolean = other match {
    case that: OscarsClient => this.endpoint == that.endpoint &&
      this.isUTM == that.isUTM &&
      this.deduplicationPropertyJsonPath == that.deduplicationPropertyJsonPath
    case _ => false
  }

  override final def hashCode(): Int = {
    val state = Seq(endpoint, isUTM, deduplicationPropertyJsonPath)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
