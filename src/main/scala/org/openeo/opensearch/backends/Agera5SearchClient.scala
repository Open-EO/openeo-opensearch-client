package org.openeo.opensearch.backends

import com.google.common.cache.{CacheBuilder, CacheLoader}
import geotrellis.proj4.LatLng
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.store.hadoop.util.HdfsUtils
import geotrellis.vector.ProjectedExtent
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.openeo.opensearch.OpenSearchResponses.Link
import org.openeo.opensearch.{OpenSearchClient, OpenSearchResponses}
import org.slf4j.LoggerFactory

import java.net.URI
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util
import java.util.concurrent.TimeUnit.HOURS
import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter
import scala.util.matching.Regex


class Agera5SearchClient(val dataGlob: String, val bands: util.List[String], val dateRegex: Regex) extends OpenSearchClient {
  private val crs = LatLng

  private val logger = LoggerFactory.getLogger(classOf[OpenSearchClient])

  protected def deriveDate(filename: String, date: Regex): ZonedDateTime = filename match {
    case date(year, month, day) => LocalDate.of(year.toInt, month.toInt, day.toInt).atStartOfDay(ZoneId.of("UTC"))
    case _ => {logger.warn(s"Agera5 products $filename failed to match regex: ${date.toString()}"); null}
  }

  private def getBandFiles(dewPointTemperatureFile: String): Seq[(String, String)] = {
    val dewPointTemperatureMarker = "dewpoint-temperature"

    require(dewPointTemperatureFile contains dewPointTemperatureMarker)

    bands
      .asScala
      .toArray
      .map(replacement => (dewPointTemperatureFile.replace(dewPointTemperatureMarker, replacement), replacement))
  }

  private val pathsCache = CacheBuilder
    .newBuilder()
    .expireAfterWrite(1, HOURS)
    .build(new CacheLoader[String, List[Path]] {
      override def load(dataGlob: String): List[Path] =
        HdfsUtils.listFiles(new Path(s"file:$dataGlob"), new Configuration)
    })
  protected def paths: List[Path] = pathsCache.get(dataGlob)

  // Note: All parameters except for dateRange are unused.
  override def getProducts(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String): Seq[OpenSearchResponses.Feature] = {
    val datedPaths: List[(ZonedDateTime, String)] = paths
      .map(path => deriveDate(path.toUri.getPath, dateRegex) -> path.toUri.getPath).filter(_._1!=null)

    var sortedDates = datedPaths
      .toArray
      .sortWith { case ((d1, _), (d2, _)) => d1 isBefore d2 }

    if (dateRange.isDefined) {
      val from = dateRange.get._1
      val to = dateRange.get._2
      sortedDates = sortedDates
        .dropWhile { case (date, _) => date isBefore from }
        .takeWhile { case (date, _) => !(date isAfter to) }
    }

    val datedRasterSources: Array[(ZonedDateTime, String, GeoTiffRasterSource)] = sortedDates
      .map { case (date, path) =>
        val bandRasterSourcePath: String = getBandFiles(path).head._1
        val bandRasterSource = GeoTiffRasterSource(bandRasterSourcePath)
        (date, path, bandRasterSource)
      }

    val features: Array[OpenSearchResponses.Feature] = datedRasterSources.map{ case (date: ZonedDateTime, path: String, source: GeoTiffRasterSource) =>
      val links = getBandFiles(path).map { case (file, band) => Link(URI.create(s"""$file"""), Some(band)) }
      OpenSearchResponses.Feature(s"${path}", source.extent, date, links.toArray, Some(source.gridExtent.cellSize.width), new OpenSearchResponses.GeneralProperties(), None,None, crs = Some(crs))
    }

    features.toSeq
  }

  override def getCollections(correlationId: String): Seq[OpenSearchResponses.Feature] = ???
  override protected def getProductsFromPage(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String, page: Int): OpenSearchResponses.FeatureCollection = ???
}