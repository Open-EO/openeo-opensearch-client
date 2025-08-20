package org.openeo.opensearch.backends

import com.google.common.cache.{CacheBuilder, CacheLoader}
import geotrellis.proj4.LatLng
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.store.hadoop.util.HdfsUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.openeo.opensearch.OpenSearchResponses.Link
import org.openeo.opensearch.{OpenSearchClient, OpenSearchResponses}
import org.slf4j.LoggerFactory

import java.net.URI
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util
import java.util.concurrent.TimeUnit.HOURS
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex


object Agera5SearchClient{
  private val logger = LoggerFactory.getLogger(classOf[OpenSearchClient])

  def apply(endpoint: String, isUTM: Boolean, dateRegex: String, bands: util.List[String]): OpenSearchClient = {
    new Agera5SearchClient(endpoint, bands, dateRegex.r.unanchored)

  }
  def apply(endpoint: String, isUTM: Boolean, dateRegex: String, bands: util.List[String], bandMarker: String): OpenSearchClient = {
      new Agera5SearchClient(endpoint, bands, dateRegex.r.unanchored, bandMarker)
  }

  def create(endpoint: String, isUTM: Boolean, dateRegex: String, bands: util.List[String], bandMarker: String): OpenSearchClient = {
    new Agera5SearchClient(endpoint, bands, dateRegex.r.unanchored, bandMarker)
  }
}

class Agera5SearchClient(val dataGlob: String, val bands: util.List[String], val dateRegex: Regex, val bandMarker:String = "dewpoint-temperature" ) extends OpenSearchClient {
  import Agera5SearchClient._

  require(dataGlob != null)
  require(bands != null)
  require(dateRegex != null)
  require(bandMarker != null)

  protected def deriveDate(filename: String, date: Regex): ZonedDateTime = filename match {
    case date(year, month, day) => LocalDate.of(year.toInt, month.toInt, day.toInt).atStartOfDay(ZoneId.of("UTC"))
    case _ => {logger.warn(s"Agera5 products $filename failed to match regex: ${date.toString()}"); null}
  }

  private def getBandFiles(dewPointTemperatureFile: String): Seq[(String, String)] = {

    require(dewPointTemperatureFile contains bandMarker)

    bands
      .asScala
      .toArray
      .map(replacement => (dewPointTemperatureFile.replace(bandMarker, replacement), replacement))
  }

  // TODO: move cache to the companion object?
  private val pathsCache = CacheBuilder
    .newBuilder()
    .expireAfterWrite(1, HOURS)
    .build(new CacheLoader[String, List[Path]] {
      override def load(dataGlob: String): List[Path] =
        HdfsUtils.listFiles(new Path(s"file:$dataGlob"), new Configuration)
    })

  private def paths: List[Path] = pathsCache.get(dataGlob)

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
        .takeWhile { case (date, _) => (date isBefore to) || (date isEqual from) }
    }

    val datedRasterSources: Array[(ZonedDateTime, String, GeoTiffRasterSource)] = sortedDates
      .map { case (date, path) =>
        val bandRasterSourcePath: String = getBandFiles(path).head._1
        val bandRasterSource = GeoTiffRasterSource(bandRasterSourcePath)
        (date, path, bandRasterSource)
      }

    val features: Array[OpenSearchResponses.Feature] = datedRasterSources.map{ case (date: ZonedDateTime, path: String, source: GeoTiffRasterSource) =>
      val links = getBandFiles(path).map { case (file, band) => Link(URI.create(s"""$file"""), Some(band)) }
      OpenSearchResponses.Feature(path, source.extent, date, links.toArray, Some(source.gridExtent.cellSize.width),
        tileID = None, geometry = None, crs = Some(LatLng))
    }

    features.toSeq
  }

  override def getCollections(correlationId: String): Seq[OpenSearchResponses.Feature] = {
    val worldExtent = Extent(-180.0, -90.0, 180.0, 90.0)
    Seq(OpenSearchResponses.Feature(
      "agera5:" + dataGlob,
      worldExtent,
      ZonedDateTime.parse("2020-01-01T00:00:00Z"),
      Array(),
      Option.empty,
      None,
      geometry=Some(worldExtent.toPolygon()),
      crs=Option.empty,
      rasterExtent = Option.empty
    ))

  }

  override protected def getProductsFromPage(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String, page: Int): OpenSearchResponses.FeatureCollection = {
    val products = getProducts(collectionId, dateRange, bbox, attributeValues, correlationId, processingLevel).toArray
    OpenSearchResponses.FeatureCollection(products.length, products)
  }


  override final def equals(other: Any): Boolean = other match {
    case that: Agera5SearchClient =>
        this.dataGlob == that.dataGlob &&
        this.bands == that.bands &&
        // Scala Regex and underlying Java Pattern do not implement equals() and hashCode()
        this.dateRegex.pattern.pattern() == that.dateRegex.pattern.pattern() &&
        this.dateRegex.pattern.flags() == that.dateRegex.pattern.flags() &&
        this.bandMarker == that.bandMarker
    case _ => false
  }

  override final def hashCode(): Int = {
    // see remark in equals()
    val state = Seq(dataGlob, bands, dateRegex.pattern.pattern(), dateRegex.pattern.flags(), bandMarker)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
