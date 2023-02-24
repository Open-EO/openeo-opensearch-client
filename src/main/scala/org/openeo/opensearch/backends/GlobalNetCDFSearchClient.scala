package org.openeo.opensearch.backends

import com.google.common.cache.{CacheBuilder, CacheLoader}
import geotrellis.proj4.LatLng
import geotrellis.raster.GridExtent
import geotrellis.raster.gdal.{GDALRasterSource, GDALWarpOptions}
import geotrellis.store.hadoop.util.HdfsUtils
import geotrellis.vector.ProjectedExtent
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.openeo.opensearch.OpenSearchResponses.Link
import org.openeo.opensearch.{OpenSearchClient, OpenSearchResponses}

import java.net.URI
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util
import java.util.concurrent.TimeUnit.HOURS
import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter
import scala.util.matching.Regex

class GlobalNetCDFSearchClient(val dataGlob: String, val bands: util.List[String], val dateRegex: Regex, val gridExtent:Option[GridExtent[Long]]= Option.empty) extends OpenSearchClient {

  protected def deriveDate(filename: String, date: Regex): ZonedDateTime = filename match {
    case date(year, month, day) => LocalDate.of(year.toInt, month.toInt, day.toInt).atStartOfDay(ZoneId.of("UTC"))
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
    val datedPaths: Map[ZonedDateTime, String] = paths
      .map(path => deriveDate(path.toUri.getPath, dateRegex) -> path.toUri.getPath)
      .groupBy { case (date, _) => date }
      .mapValues(_.map { case (_, path) => path }.max) // take RTxs into account

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

    if(gridExtent.isEmpty) {
      val datedRasterSources: Array[(ZonedDateTime, String, GDALRasterSource)] = sortedDates
        .flatMap { case (date, path) => bands.asScala.map(v => (date, path, GDALRasterSource(s"""NETCDF:"$path":$v""", GDALWarpOptions(alignTargetPixels = false)))) }

      val features: Array[OpenSearchResponses.Feature] = datedRasterSources.map { case (date: ZonedDateTime, path: String, source: GDALRasterSource) =>
        OpenSearchResponses.Feature(s"${path}", source.extent, date, bands.asScala.map(v => Link(URI.create(s"""NETCDF:$path:$v"""), Some(v))).toArray, Some(source.gridExtent.cellSize.width.toInt), None)
      }

      OpenSearchResponses.dedupFeatures(features).toSeq
    }else{
      val datedRasterSources: Array[(ZonedDateTime, String)] = sortedDates
        .flatMap { case (date, path) => bands.asScala.map(v => (date, path)) }

      val features: Array[OpenSearchResponses.Feature] = datedRasterSources.map { case (date: ZonedDateTime, path: String) =>
        OpenSearchResponses.Feature(s"${path}", gridExtent.get.extent, date, bands.asScala.map(v => Link(URI.create(s"""NETCDF:$path:$v"""), Some(v))).toArray, Some(gridExtent.get.cellSize.width.toInt), None,geometry=None,crs=Some(LatLng),rasterExtent = Some(gridExtent.get.extent))
      }

      OpenSearchResponses.dedupFeatures(features).toSeq

    }

  }

  override def getCollections(correlationId: String): Seq[OpenSearchResponses.Feature] = ???
  override protected def getProductsFromPage(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String, page: Int): OpenSearchResponses.FeatureCollection = ???
}