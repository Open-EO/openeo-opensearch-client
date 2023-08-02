package org.openeo.opensearch

import geotrellis.vector.ProjectedExtent
import org.openeo.opensearch.OpenSearchResponses.{Feature, FeatureCollection}
import org.openeo.opensearch.backends._
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpOptions, HttpRequest}

import java.io.IOException
import java.net.URL
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, OffsetTime, ZonedDateTime}
import java.util
import java.util.concurrent.atomic.AtomicLong
import scala.collection.Map

/**
 *
 * WARNING: copy to avoid cyclic dependencies.
 * We may need a separate scala opensearch library.
 */
object OpenSearchClient {
  private val logger = LoggerFactory.getLogger(classOf[OpenSearchClient])
  private val requestCounter = new AtomicLong

  def apply(endpoint: URL, isUTM: Boolean = false, clientType: String = ""):OpenSearchClient = {
    if (clientType != "") {
      clientType.toLowerCase() match {
        case "stac" => new STACClient(endpoint, false)
        case "stacs3" => new STACClient(endpoint, true)
        case "creodias" => new CreodiasClient(endpoint)
        case "oscars" => new OscarsClient(endpoint, isUTM)
        case _ => throw new IllegalArgumentException(s"Unknown catalog type: $clientType")
      }
    } else {
      // Guess Catalog Type using URL.
      endpoint.toString match {
        case s if s.contains("creo") => new CreodiasClient(endpoint)
        case s if s.contains("catalogue.dataspace.copernicus.eu/resto") => new CreodiasClient(endpoint)
        case s if s.contains("aws") => new STACClient(endpoint)
        case s if s.contains("c-scale") => new STACClient(endpoint, false)
        case _ => new OscarsClient(endpoint, isUTM)
      }
    }
  }


  /**
   *  Create a new OpenSearchClient.
   *
   *  @param endpoint An URL or a glob pattern.
   *  @param isUTM Whether the requested features are in UTM.
   *               This is an issue with some OSCARS catalogs where they don't specify the UTM zone.
   *               If isUTM is true, the UTM crs is guessed from the feature's extent.
   *               This parameter is only used for the OscarsClient.
   *  @param dateRegex A regex to extract the date from a filename.
*                      This parameter is only used for glob based endpoints.
   *  @param bands A list of bands to extract from the files.
   *               This parameter is only used for glob based endpoints.
   *  @param clientType The type of client to use.
   *                    Currently supported:
   *                      Glob based: "cgls", "agera5", "globspatialonly".
   *                      URL based: "stac", "creodias", "oscars".
   *                    This parameter is only used for glob based endpoints.
   *  @return An OpenSearchClient.
   *  @throws IllegalArgumentException if the catalogType is unknown.
   */
  def apply(endpoint: String, isUTM: Boolean, dateRegex: String, bands: util.List[String], clientType: String): OpenSearchClient = {
    clientType match {
      case "cgls" => new GlobalNetCDFSearchClient(endpoint, bands, dateRegex.r.unanchored)
      case "agera5" => new Agera5SearchClient(endpoint, bands, dateRegex.r.unanchored)
      case "globspatialonly" => new GeotiffNoDateSearchClient(endpoint, bands)
      case _ => apply(new URL(endpoint), isUTM, clientType)
    }
  }
}

abstract class OpenSearchClient {
  import OpenSearchClient._

  def getProducts(collectionId: String,
                  dateRange : (LocalDate, LocalDate),
                  bbox: ProjectedExtent,
                  attributeValues: Map[String, Any], correlationId: String,
                  processingLevel: String): Seq[Feature] = {
    // Convert LocalDates to ZonedDateTime.
    val endOfDay = OffsetTime.of(23, 59, 59, 999999999, UTC)

    val start = dateRange._1.atStartOfDay(UTC)
    val end = dateRange._2.atTime(endOfDay).toZonedDateTime

    getProducts(collectionId, Some((start, end)), bbox, attributeValues, correlationId = correlationId, processingLevel)
  }

  def getProducts(collectionId: String,
                  dateRange: Option[(ZonedDateTime, ZonedDateTime)],
                  bbox: ProjectedExtent,
                  attributeValues: Map[String, Any] = Map(), correlationId: String = "",
                  processingLevel: String = ""): Seq[Feature]

  def getProducts(collectionId: String,
                  dateRange: Option[(ZonedDateTime, ZonedDateTime)],
                  bbox: ProjectedExtent,
                  correlationId: String,
                  processingLevel: String) : Seq[Feature] =
    getProducts(collectionId, dateRange, bbox, Map[String, Any](), correlationId=correlationId, processingLevel)

  protected def getProductsFromPage(collectionId: String,
                            dateRange: Option[(ZonedDateTime, ZonedDateTime)],
                            bbox: ProjectedExtent,
                            attributeValues: Map[String, Any], correlationId: String,
                            processingLevel: String,
                            page: Int): FeatureCollection

  def getCollections(correlationId: String = ""): Seq[Feature]

  protected def http(url: String): HttpRequest =
    Http(url)
      .option(HttpOptions.followRedirects(true))
      .timeout(connTimeoutMs = 10000, readTimeoutMs = 40000)

  protected def clientId(correlationId: String): String = {
    if (correlationId.isEmpty) correlationId
    else {
      val count = requestCounter.getAndIncrement()
      s"${correlationId}_$count"
    }
  }

  protected def execute(request: HttpRequest): String = withRetries {
    val url = request.urlBuilder(request)

    val response = request
      .timeout(connTimeoutMs = 10000, readTimeoutMs = 5 * 60 * 1000) // 5min, as catalogue API can be realy slow
      .asString

    logger.info(s"$url returned ${response.code}")
    if(response.isError) {
      if(response.contentType.contains("application/json") || response.contentType.contains("application/geo+json;charset=UTF-8")) {
        io.circe.parser.parse(response.body) match {
          case Left(failure) => throw new IOException(s"Exception while evaluating catalog request $url: ${response.body}")
          case Right(json) => throw new IOException(s"Exception while evaluating catalog request $url: ${json.findAllByKey("exceptionText").mkString(";")} ")
        }

        throw new IOException(s"$url returned an empty body")
      }else{
        throw new IOException(s"Exception while evaluating catalog request $url: ${response.body}")

      }
    }else{
      val json = response.body // note: the HttpStatusException's message doesn't include the response body

      if (json.trim.isEmpty) {
        throw new IOException(s"$url returned an empty body")
      }
      json
    }
  }

  def equals(obj: Any): Boolean
  def hashCode(): Int
}
