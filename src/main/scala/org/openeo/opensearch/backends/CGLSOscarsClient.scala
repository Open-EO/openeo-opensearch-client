package org.openeo.opensearch.backends

import geotrellis.vector.ProjectedExtent
import org.openeo.opensearch.OpenSearchResponses
import org.openeo.opensearch.OpenSearchResponses.Link

import java.net.{URI, URL}
import java.time.ZonedDateTime
import java.util
import scala.jdk.CollectionConverters._

class CGLSOscarsClient(override val endpoint: URL, val bands: util.List[String]) extends OscarsClient(
  endpoint, false, deduplicationPropertyJsonPath = "properties.productInformation.productGroupId") {

  override def getProducts(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String): Seq[OpenSearchResponses.Feature] = {

    val features = super.getProducts(collectionId, dateRange, bbox, attributeValues, correlationId, processingLevel)
    features.map(f => {
      val newLinks = f.links.flatMap(l => {

        if (l.href.toString.endsWith(".nc")) {
          val path = l.href.getPath
          bands.asScala.map(b => Link(URI.create(s"""NETCDF:$path:$b"""), Some(b)))
        } else {
          Some(l)
        }

      })
      f.copy(links = newLinks)
    })
  }
}
