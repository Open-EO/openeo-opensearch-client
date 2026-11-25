package org.openeo

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector._
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.exception.SdkClientException

import java.io.IOException
import java.net.SocketTimeoutException
import java.net.http.HttpTimeoutException
import java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME
import java.time.{Duration, Instant, ZonedDateTime}
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS
import scala.annotation.tailrec
import scala.util.{Random, Try}

package object opensearch {
  private val logger = LoggerFactory.getLogger("opensearch")

  // TODO: put it in a central place
  implicit object ZonedDateTimeOrdering extends Ordering[ZonedDateTime] {
    override def compare(x: ZonedDateTime, y: ZonedDateTime): Int = x compareTo y
  }

  /** Non-success HTTP response. `retryAfter` is the server-requested wait (Retry-After header), if any. */
  class HttpStatusException(val code: Int, message: String, val retryAfter: Option[Duration] = None,
                            cause: Throwable = null) extends IOException(message, cause)

  /** Parses a Retry-After header value: either delay-seconds or an HTTP-date. */
  def parseRetryAfter(value: String, now: Instant = Instant.now()): Option[Duration] = {
    val trimmed = value.trim
    val duration =
      if (trimmed.nonEmpty && trimmed.forall(_.isDigit)) Try(Duration.ofSeconds(trimmed.toLong)).toOption
      else Try(Duration.between(now, ZonedDateTime.parse(trimmed, RFC_1123_DATE_TIME).toInstant)).toOption
    duration.map(d => if (d.isNegative) Duration.ZERO else d)
  }

  private val maxRetryAfter = Duration.ofMinutes(2)

  def withRetries[R](action: => R): R = {
    @tailrec
    def attempt(retries: Int, delay: (Long, TimeUnit))(action: => R): R = {
      val (amount, timeUnit) = delay
      var sleepMillis = timeUnit.toMillis(amount)

      def retryable(e: Exception): Boolean = retries > 0 && (e match {
        case h: HttpStatusException if h.code == 429 =>
          // honour Retry-After (capped), otherwise fall back to the exponential backoff; jitter avoids synchronized retries
          val base = h.retryAfter.map(d => math.min(d.toMillis, maxRetryAfter.toMillis)).getOrElse(sleepMillis)
          sleepMillis = base + Random.nextInt(1000)
          logger.warn(s"rate limited (429): retrying in $sleepMillis ms")
          true
        case h: HttpStatusException if h.code >= 500 => true
        case e: IOException if e.getMessage.endsWith("returned an empty body") =>
          logger.warn(s"encountered empty body: retrying within $amount $timeUnit", e)
          true
        case e: IOException if e.getMessage.contains("catalogue.dataspace.copernicus.eu") =>
          // Mostly error code 500, but keeping it open just in case.
          logger.warn(s"Errors for this API can be retried: retrying within $amount $timeUnit", e)
          true
        case _: SocketTimeoutException | _: HttpTimeoutException =>
          logger.warn(s"socket timeout exception: retrying within $amount $timeUnit")
          true
        case e: SdkClientException =>
          logger.warn(s"Retrying: ${e.getMessage} Retryable: ${e.retryable()}")
          true
        case _ => false
      })

      try
        return action
      catch {
        case e: Exception if retryable(e) => ()
      }

      Thread.sleep(sleepMillis)
      attempt(retries - 1, (amount * 2, timeUnit)) {
        action
      }
    }

    attempt(retries = 4, delay = (5, SECONDS)) {
      action
    }
  }


  /**
   * Will give the same angle meaning, but as positive value.
   */
  def to_0_360_range(x: Double): Double = {
    (x + 360 * 10) % 360
  }

  def safeReproject(inputProjectedExtent: ProjectedExtent, targetCrs: CRS): ProjectedExtent = {
    if (inputProjectedExtent.crs == targetCrs) return inputProjectedExtent
    var reprojected = inputProjectedExtent.extent.reproject(inputProjectedExtent.crs, targetCrs)
    // TODO: Needed for webmercator too?
    if (targetCrs == LatLng && reprojected.width > 180 && reprojected.width < 360) {
      // Fix width wrap when projecting over anti meridian in LatLon.
      // Reprojecting an extent could make the left and the right side swap differently over the antimeridian.
      // A single point will always work, so consider that as the source of truth
      // When we experience a problem because the extent does not fit in [-180, 180] range, convert it to the [0-360] range.
      val centerReprojected = inputProjectedExtent.extent.center.reproject(inputProjectedExtent.crs, targetCrs)
      val reprojectedCenter = reprojected.center
      if (Math.abs(centerReprojected.x - reprojectedCenter.x) > 10) {
        val swapped = Extent(to_0_360_range(reprojected.xmax), reprojected.ymin, to_0_360_range(reprojected.xmin), reprojected.ymax)
        val swappedCenter = swapped.center
        if (Math.abs(to_0_360_range(centerReprojected.x) - swappedCenter.x) < 10) {
          reprojected = swapped
        }
      }
    }
    ProjectedExtent(reprojected, targetCrs)
  }
}
