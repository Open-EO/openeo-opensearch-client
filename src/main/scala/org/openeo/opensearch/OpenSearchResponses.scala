package org.openeo.opensearch

import _root_.io.circe.parser.decode
import cats.syntax.either._
import cats.syntax.show._
import geotrellis.proj4.CRS
import io.circe.generic.auto._
import io.circe.{Decoder, HCursor, Json}
import geotrellis.vector._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}

import java.io.{FileInputStream, FileNotFoundException}
import java.lang.System.getenv
import java.net.URI
import java.nio.file.Paths
import java.time.ZonedDateTime
import java.util.regex.Pattern
import javax.net.ssl.HttpsURLConnection
import scala.xml.{Node, XML}

object OpenSearchResponses {
  implicit val decodeUrl: Decoder[URI] = Decoder.decodeString.map(URI.create)
  implicit val decodeDate: Decoder[ZonedDateTime] = Decoder.decodeString.map(s => ZonedDateTime.parse(s.split('/')(0)))

  case class Link(href: URI, title: Option[String])

  case class Feature(id: String, bbox: Extent, nominalDate: ZonedDateTime, links: Array[Link], resolution: Option[Int],
                     tileID: Option[String] = None){
    val crs: Option[CRS] = for {
      id <- tileID if id.matches("[0-9]{2}[A-Z]{3}")
      utmEpsgStart = if (id.charAt(2) >= 'N') "326" else "327"
    } yield CRS.fromEpsgCode((utmEpsgStart + id.substring(0, 2)).toInt)
  }

  case class FeatureCollection(itemsPerPage: Int, features: Array[Feature])

  object FeatureCollection {
    def parse(json: String): FeatureCollection = {
      implicit val decodeFeature: Decoder[Feature] = new Decoder[Feature] {
        override def apply(c: HCursor): Decoder.Result[Feature] = {
          for {
            id <- c.downField("id").as[String]
            bbox <- c.downField("bbox").as[Array[Double]]
            nominalDate <- c.downField("properties").downField("date").as[ZonedDateTime]
            links <- c.downField("properties").downField("links").as[Map[String, Array[Link]]]
            resolution = c.downField("properties").downField("productInformation").downField("resolution").downArray.first.as[Int].toOption
            tileId = c.downField("properties").downField("acquisitionInformation").downAt(_.hcursor.downField("acquisitionParameters").succeeded).downField("acquisitionParameters").downField("tileId").as[String].toOption
          } yield {
            val Array(xMin, yMin, xMax, yMax) = bbox
            val extent = Extent(xMin, yMin, xMax, yMax)

            Feature(id, extent, nominalDate, links.values.flatten.toArray, resolution,tileId)
          }
        }
      }

      decode[FeatureCollection](json)
        .valueOr(e => throw new IllegalArgumentException(s"${e.show} while parsing '$json'", e))
    }
  }

  object STACFeatureCollection {
    def parse(json: String, toS3URL:Boolean=true): FeatureCollection = {
      implicit val decodeFeature: Decoder[Feature] = new Decoder[Feature] {
        override def apply(c: HCursor): Decoder.Result[Feature] = {
          for {
            id <- c.downField("id").as[String]
            bbox <- c.downField("bbox").as[Array[Double]]
            nominalDate <- c.downField("properties").downField("datetime").as[ZonedDateTime]
            links <- c.downField("assets").as[Map[String, Link]]
            resolution = c.downField("properties").downField("gsd").as[Int].toOption
          } yield {
            val Array(xMin, yMin, xMax, yMax) = bbox
            val extent = Extent(xMin, yMin, xMax, yMax)

            val harmonizedLinks = links.map { t =>
              val href = t._2.href
              if(toS3URL){
                val bucket = href.getHost.split('.')(0)
                val s3href = URI.create("s3://" + bucket +href.getPath)
                Link(s3href, Some(t._1))
              }
              else{
                Link(href, Some(t._1)) }
            }
            Feature(id, extent, nominalDate, harmonizedLinks.toArray, resolution,None)
          }
        }
      }

      implicit val decodeFeatureCollection: Decoder[FeatureCollection] = new Decoder[FeatureCollection] {
        override def apply(c: HCursor): Decoder.Result[FeatureCollection] = {
          for {
            itemsPerPage <- c.downField("numberReturned").as[Int]
            features <- c.downField("features").as[Array[Feature]]
          } yield {
            FeatureCollection(itemsPerPage, features)
          }
        }
      }

      decode[FeatureCollection](json)
        .valueOr(e => throw new IllegalArgumentException(s"${e.show} while parsing '$json'", e))
    }
  }

  object CreoFeatureCollection {

    private val TILE_PATTERN = Pattern.compile("_T([0-9]{2}[A-Z]{3})_")
    private val s3Endpoint = System.getenv().getOrDefault("AWS_S3_ENDPOINT","")//https://s3.cloudferro.com
    private val creoClient = {
      if(s3Endpoint!="") {
        Some(S3Client.builder.endpointOverride(new URI(s3Endpoint)).region(Region.of("RegionOne"))
          .serviceConfiguration(S3Configuration.builder.pathStyleAccessEnabled(true).build).build())
      }else{
        Option.empty
      }
    }

    private def getAwsDirect() = {
      "TRUE".equals(getenv("AWS_DIRECT"))
    }

    private def getFilePathsFromManifest(path: String): Seq[Link] = {
      var gdalPrefix = ""

      val inputStream = if (path.startsWith("https://")) {
        gdalPrefix = "/vsicurl"

        val uri = new URI(path)
        uri.resolve(s"${uri.getPath}/manifest.safe").toURL
          .openConnection.asInstanceOf[HttpsURLConnection]
          .getInputStream
      } else {
        gdalPrefix = if (getAwsDirect()) "/vsis3" else ""

        if(path.startsWith("/eodata")) {

          //reading from /eodata is extremely slow
          if(creoClient.isDefined) {
            creoClient.get.getObject(GetObjectRequest.builder().bucket("eodata").key(s"${path.toString.replace("/eodata/","")}/manifest.safe").build())
          }else{
            val url = path.replace("/eodata","https://finder.creodias.eu/files")
            val uri = new URI(url)
            try {
              uri.resolve(s"${uri.getPath}/manifest.safe").toURL
                .openConnection.asInstanceOf[HttpsURLConnection]
                .getInputStream
            } catch {
              case e: FileNotFoundException => return Seq.empty[Link]
            }
          }

        }else{
          new FileInputStream(Paths.get(path, "manifest.safe").toFile)
        }
      }

      val xml = XML.load(inputStream)


      (xml \\ "dataObject" )
        .map((dataObject: Node) =>{
          val title = dataObject \\ "@ID"
          val fileLocation = dataObject \\ "fileLocation" \\ "@href"
          Link(URI.create(s"$gdalPrefix${if (path.startsWith("/")) "" else "/"}$path" + s"/${Paths.get(fileLocation.toString).normalize().toString}"),Some(title.toString))
        })
    }

    def parse(json: String): FeatureCollection = {
      implicit val decodeFeature: Decoder[Feature] = new Decoder[Feature] {
        override def apply(c: HCursor): Decoder.Result[Feature] = {
          for {
            id <- c.downField("properties").downField("productIdentifier").as[String]
            geometry <- c.downField("geometry").as[Json]
            nominalDate <- c.downField("properties").downField("startDate").as[ZonedDateTime]
            links <- c.downField("properties").downField("links").as[Array[Link]]
            resolution = c.downField("properties").downField("resolution").as[Int].toOption
          } yield {

            val extent = geometry.toString().parseGeoJson[Geometry].extent
            val tileIDMatcher = TILE_PATTERN.matcher(id)
            val tileID =
              if(tileIDMatcher.find()){
                Some(tileIDMatcher.group(1))
              }else{
                Option.empty
              }

            if(id.endsWith(".SAFE")){
              val all_links = getFilePathsFromManifest(id)
              Feature(id, extent, nominalDate, all_links.toArray, resolution,tileID)
            }else{
              Feature(id, extent, nominalDate, links, resolution,tileID)
            }

          }
        }
      }

      implicit val decodeFeatureCollection: Decoder[FeatureCollection] = new Decoder[FeatureCollection] {
        override def apply(c: HCursor): Decoder.Result[FeatureCollection] = {
          for {
            itemsPerPage <- c.downField("properties").downField("itemsPerPage").as[Int]
            features <- c.downField("features").as[Array[Feature]]
          } yield {
            FeatureCollection(itemsPerPage, features)
          }
        }
      }

      decode[FeatureCollection](json)
        .valueOr(e => throw new IllegalArgumentException(s"${e.show} while parsing '$json'", e))
    }
  }

  case class STACCollection(id: String)
  case class STACCollections(collections: Array[STACCollection])

  object STACCollections {
    def parse(json: String): STACCollections = {
      decode[STACCollections](json)
        .valueOr(e => throw new IllegalArgumentException(s"${e.show} while parsing '$json'", e))
    }
  }

  case class CreoCollection(name: String)
  case class CreoCollections(collections: Array[CreoCollection])

  object CreoCollections {
    def parse(json: String): CreoCollections = {
      decode[CreoCollections](json)
        .valueOr(e => throw new IllegalArgumentException(s"${e.show} while parsing '$json'", e))
    }
  }
}
