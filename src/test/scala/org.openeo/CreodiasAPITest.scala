package org.openeo

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector.{Extent, Geometry, MultiPolygon, Polygon, ProjectedExtent}
import io.circe.{Json, ParsingFailure, parser}
import org.junit.{Assert, Test}
import org.locationtech.jts.geom.Coordinate
import scalaj.http.{Http, HttpOptions, HttpRequest}
import geotrellis.vector._

import java.io.IOException
import java.time.ZoneOffset.UTC
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.ISO_INSTANT
import scala.collection.Map

class CreodiasAPITest {

  def getProductsFromPage(collectionId: String,
                          dateRange: Option[(ZonedDateTime, ZonedDateTime)],
                          bbox: ProjectedExtent,
                          attributeValues: Map[String, Any], correlationId: String,
                          processingLevel: String, page: Int): String = {
    val Extent(xMin, yMin, xMax, yMax) = bbox.reproject(LatLng)
    val collection = s"https://finder.creodias.eu/resto/api/collections/$collectionId/search.json"
    val http = Http(collection).option(HttpOptions.followRedirects(true))
    var getProducts = http
      .param("processingLevel", processingLevel)
      .param("box", Array(xMin, yMin, xMax, yMax) mkString ",")
      .param("sortParam", "startDate") // paging requires deterministic order
      .param("sortOrder", "ascending")
      .param("page", page.toString)
      .param("maxRecords", "100")
      .param("status", "0|34|37")
      .param("dataset", "ESA-DATASET")
      .params(attributeValues.mapValues(_.toString).filterKeys(!Seq( "eo:cloud_cover", "provider:backend", "orbitDirection", "sat:orbit_state").contains(_)).toSeq)
    if (dateRange.isDefined) {
      getProducts = getProducts
        .param("startDate", dateRange.get._1 format ISO_INSTANT)
        .param("completionDate", dateRange.get._2 format ISO_INSTANT)
    }
    if( "Sentinel1".equals(collectionId)) {
      getProducts = getProducts.param("productType","GRD")
    }
    def execute(request: HttpRequest): String = {
      val url = request.urlBuilder(request)
      val response = request.asString
      val json = response.throwError.body // note: the HttpStatusException's message doesn't include the response body
      if (json.trim.isEmpty) {
        throw new IOException(s"$url returned an empty body")
      }
      json
    }
    val json = execute(getProducts)
    json
  }

  @Test
  def testCreoGeometry(): Unit = {
    /*
    The old Creodias API can sometimes return MultiPolygon geometry that is not correctly formatted.
    */

    // Create reference.
    val polygon = Polygon(Seq[Coordinate](
      new Coordinate(3.179215, 50.227345),
      new Coordinate(3.666933, 51.720463),
      new Coordinate(0, 52.120404086457),
      new Coordinate(-0.091521, 52.130386),
      new Coordinate(-0.460228, 50.635101),
      new Coordinate(0, 50.583537964566),
      new Coordinate(3.179215, 50.227345)
    ))
    val multiPolygon = MultiPolygon(polygon)
    val reference_geojson = multiPolygon.toGeoJson()

    // Send a request to Creodias API for actual.
    val fromDate: ZonedDateTime = ZonedDateTime.of(2020, 6, 6, 0, 0, 0, 0, UTC)
    val toDate: ZonedDateTime = ZonedDateTime.of(2020, 6, 6, 23, 59, 59, 999999999, UTC)
    val crs: CRS = CRS.fromName("EPSG:32631")
    val bbox = ProjectedExtent(Extent(506961.00315656577, 5679855.354590951, 520928.44779978535, 5691014.304235179), crs)
    val attributeValues = Map[String, Any]("processingLevel" -> "LEVEL1", "sensorMode" -> "IW", "productType" -> "GRD")
    val json = getProductsFromPage(
      "Sentinel1", Some(fromDate, toDate),
      bbox, attributeValues, "", "", 1
    )
    val parsed: Either[ParsingFailure, Json] = parser.parse(json)
    val c = parsed.getOrElse(Json.Null).hcursor
    val actual_geojson = c.downField("features").downArray.downField("geometry").as[Json].getOrElse(Json.Null)
    // The actual geojson will throw an error when parsed as geometry, because the coordinates are not correctly formatted.
    Assert.assertThrows(classOf[Exception], () => actual_geojson.toString().parseGeoJson[Geometry])

    // This can be fixed by adding another level of nesting to the coordinates.
    val coordinates = c.downField("features").downArray.downField("geometry").downField("coordinates").as[Json].getOrElse(Json.Null)
    val fixed_coordinates = Json.arr(coordinates)
    val fixed_geojson = Json.obj(
      ("type", Json.fromString("MultiPolygon")),
      ("coordinates", fixed_coordinates)
    )
    // The fixed geojson can now be parsed as geometry.
    val fixed_geometry = fixed_geojson.toString().parseGeoJson[Geometry]
  }
}
