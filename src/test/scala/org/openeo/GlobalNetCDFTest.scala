package org.openeo

import geotrellis.proj4.LatLng
import geotrellis.raster.{CellSize, GridExtent}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.Assert._
import org.junit.Test
import org.openeo.opensearch.backends.GlobalNetCDFSearchClient

import java.nio.file.{Files, Path}
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util.Collections

class GlobalNetCDFTest {

  @Test
  def testLoadNetCDF(): Unit = {
    val dir = Files.createTempDirectory("cgls_temp")
    Files.createTempFile(dir,"fapar_202003010000_",".nc")
    Files.createTempFile(dir,"fapar_202003110000_",".nc")

    val start = LocalDate.of(2020, 3, 1).atStartOfDay(ZoneId.of("UTC"))
    val end = LocalDate.of(2020, 3, 11).atStartOfDay(ZoneId.of("UTC"))

    checkTemporalFilter(dir, start, end)
    checkTemporalFilter(dir, start, start)
  }

  private def checkTemporalFilter(dir: Path, start: ZonedDateTime, end: ZonedDateTime): Unit = {
    val resolution = CellSize(0.002976190476204, 0.002976190476190)
    val client = new GlobalNetCDFSearchClient(dir.toString + "/*.nc", Collections.singletonList("FAPAR"), raw"_(\d{4})(\d{2})(\d{2})0000_".r.unanchored, gridExtent = Some(GridExtent(Extent(-180.0014881, -59.9985119, 179.9985119, 80.0014881), resolution)))
    val prods = client.getProducts("fapar", Some(start, end), ProjectedExtent(Extent(-4, 50, 4, 54), LatLng))
    assertEquals(1, prods.length)
    val feature = prods.head
    assertEquals(Some(LatLng), feature.crs)
    assertEquals(start, feature.nominalDate)
    assertEquals(Some(resolution.width), feature.resolution)
  }


}
