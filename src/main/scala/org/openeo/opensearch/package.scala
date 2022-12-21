package org.openeo

import java.time.ZonedDateTime

package object opensearch {
  // TODO: put it in a central place
  implicit object ZonedDateTimeOrdering extends Ordering[ZonedDateTime] {
    override def compare(x: ZonedDateTime, y: ZonedDateTime): Int = x compareTo y
  }
}
