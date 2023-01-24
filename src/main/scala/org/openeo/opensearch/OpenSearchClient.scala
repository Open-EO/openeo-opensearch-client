package org.openeo.opensearch

import geotrellis.vector.ProjectedExtent
import org.openeo.opensearch.OpenSearchResponses.{Feature, FeatureCollection}
import org.openeo.opensearch.backends.{Agera5SearchClient, CreodiasClient, GlobalNetCDFSearchClient, OscarsClient, STACClient}
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpOptions, HttpRequest, HttpStatusException}

import java.io.IOException
import java.net.{SocketTimeoutException, URL}
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, OffsetTime, ZonedDateTime}
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec
import scala.collection.Map

/**
 *
 * WARNING: copy to avoid cyclic dependencies.
 * We may need a separate scala opensearch library.
 */
object OpenSearchClient {
  private val logger = LoggerFactory.getLogger(classOf[OpenSearchClient])
  private val requestCounter = new AtomicLong

  // = new URL("http://oscars-01.vgt.vito.be:8080")
  def apply(endpoint: URL, isUTM: Boolean = false):OpenSearchClient = {
    endpoint.toString match {
      case s if s.contains("creo") => CreodiasClient
      case s if s.contains("aws") => new STACClient(endpoint)
      case s if s.contains("c-scale") => new STACClient(endpoint, false)
      case _ => new OscarsClient(endpoint, isUTM)
    }
  }

  def apply(endpoint: String, isUTM: Boolean = false, dateRegex: String = null, variables: util.List[String] = null, clientType: String = ""): OpenSearchClient = {
    if (dateRegex!= null && variables != null) {
      clientType match {
        case "cgls" => new GlobalNetCDFSearchClient(endpoint, variables, dateRegex.r.unanchored)
        case "agera5" => new Agera5SearchClient(endpoint, variables, dateRegex.r.unanchored)
      }
    }
    apply(new URL(endpoint), isUTM)
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

  protected def http(url: String): HttpRequest = Http(url).option(HttpOptions.followRedirects(true))

  protected def clientId(correlationId: String): String = {
    if (correlationId.isEmpty) correlationId
    else {
      val count = requestCounter.getAndIncrement()
      s"${correlationId}_$count"
    }
  }

  protected def execute(request: HttpRequest): String = {
    val url = request.urlBuilder(request)
    val response = request.asString

    logger.info(s"$url returned ${response.code}")

    val json = response.throwError.body // note: the HttpStatusException's message doesn't include the response body

    if (json.trim.isEmpty) {
      throw new IOException(s"$url returned an empty body")
    }

    json
  }

  protected def withRetries[R](action: => R): R = {
    @tailrec
    def attempt[R](retries: Int, delay: (Long, TimeUnit))(action: => R): R = {
      val (amount, timeUnit) = delay

      def retryable(e: Exception): Boolean = retries > 0 && (e match {
        case h: HttpStatusException if h.code >= 500 => true
        case e: IOException if e.getMessage.endsWith("returned an empty body") =>
          logger.warn(s"encountered empty body: retrying within $amount $timeUnit", e)
          true
        case _: SocketTimeoutException =>
          logger.warn(s"socket timeout exception: retrying within $amount $timeUnit")
          true
        case _ => false
      })

      try
        return action
      catch {
        case e: Exception if retryable(e) => Unit
      }

      timeUnit.sleep(amount)
      attempt(retries - 1, (amount * 2, timeUnit)) { action }
    }

    attempt(retries = 4, delay = (5, SECONDS)) { action }
  }
}
