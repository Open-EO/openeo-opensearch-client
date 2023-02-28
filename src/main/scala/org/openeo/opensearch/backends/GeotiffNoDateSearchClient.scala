package org.openeo.opensearch.backends

import com.google.common.cache.{CacheBuilder, CacheLoader}
import geotrellis.proj4.LatLng
import geotrellis.raster.geotiff.{GeoTiffPath, GeoTiffRasterSource}
import geotrellis.store.hadoop.util.HdfsUtils
import geotrellis.vector.ProjectedExtent
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
  private val crs = LatLng

  private val logger = LoggerFactory.getLogger(classOf[OpenSearchClient])



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
  protected def paths: List[Path] = pathsCache.get(dataGlob)

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

  override def getCollections(correlationId: String): Seq[OpenSearchResponses.Feature] = ???
  override protected def getProductsFromPage(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String, page: Int): OpenSearchResponses.FeatureCollection = ???
}