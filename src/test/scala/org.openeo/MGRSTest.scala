package org.openeo

import geotrellis.vector.Extent
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.openeo.opensearch.MGRS

class MGRSTest {

  @Test
  def testMGRSToSentinel2(): Unit = {

    assertEquals(Extent(399960.0, 9890200.0, 509760.0, 1.0E7),MGRS.mgrsToSentinel2Extent("32MME"))
    assertEquals(Extent(499980.0, 7590220.0, 609780.0, 7700020.0),MGRS.mgrsToSentinel2Extent("31KES"))
    assertEquals(Extent(499980.0, 7490200.0, 609780.0, 7600000.0),MGRS.mgrsToSentinel2Extent("31KER"))
    assertEquals(Extent(499980.0, 7390240.0, 609780.0, 7500040.0),MGRS.mgrsToSentinel2Extent("31KEQ"))

    assertEquals(Extent(600000.0, 5690220.0, 709800.0, 5800020.0),MGRS.mgrsToSentinel2Extent("31UFT"))
    assertEquals(Extent(600000.0, 5590200.0, 709800.0, 5700000.0),MGRS.mgrsToSentinel2Extent("31UFS"))
    assertEquals(Extent(499980.0, 5590200.0, 609780.0, 5700000.0),MGRS.mgrsToSentinel2Extent("31UES"))
    assertEquals(Extent(399960.0, 5590200.0, 509760.0, 5700000.0),MGRS.mgrsToSentinel2Extent("31UDS"))
    assertEquals(Extent(600000.0, 5490240.0, 709800.0, 5600040.0),MGRS.mgrsToSentinel2Extent("31UFR"))
    assertEquals(Extent(600000.0, 5790240.0, 709800.0, 5900040.0),MGRS.mgrsToSentinel2Extent("31UFU"))



  }
}
