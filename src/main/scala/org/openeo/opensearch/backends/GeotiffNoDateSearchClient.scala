package org.openeo.opensearch.backends

import com.google.common.cache.{CacheBuilder, CacheLoader}
import geotrellis.proj4.LatLng
import geotrellis.raster.geotiff.{GeoTiffPath, GeoTiffRasterSource}
import geotrellis.store.hadoop.util.HdfsUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.openeo.opensearch.OpenSearchResponses.Link
import org.openeo.opensearch.{OpenSearchClient, OpenSearchResponses}
import org.slf4j.LoggerFactory

import java.net.URI
import java.time.ZonedDateTime
import java.util
import java.util.concurrent.TimeUnit.HOURS
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`


class GeotiffNoDateSearchClient(val dataGlob: String, val bands: util.List[String],  val defaultDate: String = "2020-01-01T00:00:00Z") extends OpenSearchClient {
  require(dataGlob != null)
  require(bands != null)
  require(defaultDate != null)

  // TODO: move cache to a companion object?
  private val pathsCache = CacheBuilder
    .newBuilder()
    .expireAfterWrite(1, HOURS)
    .build(new CacheLoader[String, List[Path]] {
      override def load(dataGlob: String): List[Path] = {
        if(dataGlob.startsWith("http")) {
          List(new Path(URI.create(dataGlob)))
        }else{
          HdfsUtils.listFiles(new Path(s"file:$dataGlob"), new Configuration)
        }

      }
    })

  private def paths: List[Path] = pathsCache.get(dataGlob)

  // Note: All parameters except for dateRange are unused.
  override def getProducts(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String): Seq[OpenSearchResponses.Feature] = {


    val datedRasterSources = paths
      .map { case (path) =>
        val bandRasterSource = GeoTiffRasterSource(GeoTiffPath(path.toString))
        ( path.toString, bandRasterSource)
      }

    val features = datedRasterSources.map{ case (path: String, source: GeoTiffRasterSource) =>

      OpenSearchResponses.Feature(s"${path}", source.extent.reproject(source.crs,LatLng), ZonedDateTime.parse(defaultDate), Array(Link(URI.create(s"""$path"""), bands.toSeq.headOption)), Some(source.gridExtent.cellSize.width), None,None, crs = Some(source.crs),rasterExtent = Some(source.extent))
    }

    features.toSeq
  }

  override def getCollections(correlationId: String): Seq[OpenSearchResponses.Feature] = {
    val worldExtent = Extent(-180.0, -90.0, 180.0, 90.0)
    Seq(OpenSearchResponses.Feature(
      "geotiffnodate:" + dataGlob,
      worldExtent,
      ZonedDateTime.parse(defaultDate),
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
    case that: GeotiffNoDateSearchClient =>
      this.dataGlob == that.dataGlob &&
        this.bands == that.bands &&
        this.defaultDate == that.defaultDate
    case _ => false
  }

  override final def hashCode(): Int = {
    val state = Seq(dataGlob, bands, defaultDate)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}