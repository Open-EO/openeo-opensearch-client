package org.openeo

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector._
import org.slf4j.LoggerFactory
import scalaj.http.HttpStatusException
import software.amazon.awssdk.core.exception.SdkClientException

import java.io.IOException
import java.net.SocketTimeoutException
import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS
import scala.annotation.tailrec

package object opensearch {
  private val logger = LoggerFactory.getLogger("opensearch")

  // TODO: put it in a central place
  implicit object ZonedDateTimeOrdering extends Ordering[ZonedDateTime] {
    override def compare(x: ZonedDateTime, y: ZonedDateTime): Int = x compareTo y
  }

  def withRetries[R](action: => R): R = {
    @tailrec
    def attempt(retries: Int, delay: (Long, TimeUnit))(action: => R): R = {
      val (amount, timeUnit) = delay

      def retryable(e: Exception): Boolean = retries > 0 && (e match {
        case h: HttpStatusException if h.code >= 500 => true
        case e: IOException if e.getMessage.endsWith("returned an empty body") =>
          logger.warn(s"encountered empty body: retrying within $amount $timeUnit", e)
          true
        case e: IOException if e.getMessage.contains("catalogue.dataspace.copernicus.eu") =>
          // Mostly error code 500, but keeping it open just in case.
          logger.warn(s"Errors for this API can be retried: retrying within $amount $timeUnit", e)
          true
        case _: SocketTimeoutException =>
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
        case e: Exception if retryable(e) => Unit
      }

      timeUnit.sleep(amount)
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
        if (Math.abs(centerReprojected.x - swappedCenter.x) < 10) {
          reprojected = swapped
        }
      }
    }
    ProjectedExtent(reprojected, targetCrs)
  }
}
