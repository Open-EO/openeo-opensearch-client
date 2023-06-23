package org.openeo

import org.slf4j.LoggerFactory
import scalaj.http.HttpStatusException

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
      attempt(retries - 1, (amount * 2, timeUnit)) {
        action
      }
    }

    attempt(retries = 4, delay = (5, SECONDS)) {
      action
    }
  }
}
