package org.openeo

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Paths}
import java.util.UUID

class HttpCache {

  val client: HttpClient = HttpClient.newHttpClient

  def readString(fullUrl:String): String = {
    val url = URI.create(fullUrl).toURL

    val idx = fullUrl.indexOf("//")
    val filePathOriginal = fullUrl.substring(idx + 2)
    var filePath = filePathOriginal
    val invalidChars = """[\\":*?&<>|]""".r
    filePath = invalidChars.replaceAllIn(filePath, "_") // slugify to make valid windows path
    filePath = """__+""".r.replaceAllIn(filePath, "_")

    if (filePath.length > 255 || filePath != filePathOriginal) {
      // An individual name should be max 255 characters long. Lazy implementation caps whole file path:
      val hash = "___" + (filePathOriginal.hashCode >>> 1).toString // TODO, parse extension?
      val sLength = Math.min(filePath.length, 255 - hash.length)
      filePath = filePath.substring(0, sLength) + hash
    }
    val lastSlash = filePath.lastIndexOf("/")
    val (basePath, filename) = filePath.splitAt(lastSlash + 1)
    filePath = basePath + filename

    val cachePath = """/\D:/""".r.replaceAllIn(getClass.getResource("/org/openeo/httpsCache").getPath, "/")
    // val cachePath = "src/test/resources/org/openeo/httpsCache" // Use this to cache files to git.
    val path = Paths.get(cachePath, filePath)
    if (!Files.exists(path)) {
      // HttpCache.synchronized slows down too much. A FileAlreadyExistsException is recoverable
      println("Caching request url: " + url)
      try {
        Files.createDirectories(Paths.get(cachePath, basePath))
        val tmpBeforeAtomicMove = Paths.get(cachePath, "unconfirmed_download_" + UUID.randomUUID())
        val request = HttpRequest.newBuilder()
          .uri(url.toURI)
          .build();
        client.send(request, HttpResponse.BodyHandlers.ofFile(tmpBeforeAtomicMove))

        Files.move(tmpBeforeAtomicMove, path)
        Files.readString(path)
      }
      catch {
        case e: java.io.FileNotFoundException if e.getMessage == url.toString =>
          // Those requests are not worth repeating
          println("Server returned 404 or 410: " + url)
          throw e
        case e: Throwable =>
          println("Caching error. Will retry without caching. " + e + "  " + e.getStackTrace.mkString("/n"))
          // Test this by running: chmod a=rX src/test/resources/org/openeo/httpsCache
          val request = HttpRequest.newBuilder()
            .uri(url.toURI)
            .build();

          client.send(request, HttpResponse.BodyHandlers.ofString()).body()

      }
    } else {
      println("Using cached request: " + path.toUri)
      println("Using cached request url: " + url)

      Files.readString(path)
    }
  }


}

object HttpCache {
  var enabled = false

  var randomErrorEnabled = false

  val httpsCache = new HttpCache()

}
