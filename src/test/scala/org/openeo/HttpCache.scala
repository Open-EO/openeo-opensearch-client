package org.openeo

import java.io.{File, FileInputStream, InputStream}
import java.net.URL
import java.nio.file.{Files, Paths}
import java.util.UUID

class HttpCache extends sun.net.www.protocol.https.Handler {

  /**
   * Keep reference to original method so it can be called from within HttpURLConnection.
   */
  def openConnectionSuper(url: URL): java.net.URLConnection = super.openConnection(url)

  override def openConnection(url: URL): java.net.URLConnection = {
    if (!HttpCache.enabled) {
      openConnectionSuper(url)
    } else if (url.toString.contains(".tif")) {
      println("Tiff could be requested partially, and HttpCache does not support that. " + url)
      openConnectionSuper(url)
    } else
      new java.net.HttpURLConnection(url) {
        private var inputStream: Option[InputStream] = None

        private def makeInputStream: InputStream = {
          val fullUrl = this.url.toString
          val idx = fullUrl.indexOf("//")
          val filePathOriginal = fullUrl.substring(idx + 2)
          var filePath = filePathOriginal
          val invalidChars = """[\\":*?&\"<>|]""".r
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
            HttpCache.synchronized {
              if (!Files.exists(path)) {
                println("Caching request url: " + url)
                try {
                  Files.createDirectories(Paths.get(cachePath, basePath))
                  val tmpBeforeAtomicMove = Paths.get(cachePath, "unconfirmed_download_" + UUID.randomUUID())
                  val stream = openConnectionSuper(url).getInputStream
                  try Files.copy(stream, tmpBeforeAtomicMove)
                  finally stream.close() // might save in different encodings like EUC-KR, ISO-8859-1, utf-8, ascii
                  Files.move(tmpBeforeAtomicMove, path)
                  new FileInputStream(new File(path.toString))
                }
                catch {
                  case e: java.io.FileNotFoundException if e.getMessage == url.toString =>
                    // Those requests are not worth repeating
                    println("Server returned 404 or 410: " + url)
                    throw e
                  case e: Throwable =>
                    println("Caching error. Will retry without caching. " + e + "  " + e.getStackTraceString)
                    // Test this by running: chmod a=rX src/test/resources/org/openeo/httpsCache
                    openConnectionSuper(url).getInputStream
                }
              } else {
                println("Using cached request thanks to lock") // In case the same request was made twice
                println("Using cached request: " + path.toUri)
                println("Using cached request url: " + url)
                new FileInputStream(new File(path.toString))
              }
            }
          } else {
            println("Using cached request: " + path.toUri)
            println("Using cached request url: " + url)

            // The caller should close the returned stream. For example:
            //           val source = Source.fromURL(new URL(url))
            //           val content = source.getLines.mkString("\n")
            //           source.close()
            new FileInputStream(new File(path.toString))
          }
        }

        override def getInputStream: InputStream = {
          if (inputStream.isEmpty) {
            if (HttpCache.randomErrorEnabled && HttpCache.random.nextDouble() < 0.3
              && url.toString.contains("catalogue.dataspace.copernicus.eu")) {
              println("Intentional random error for: " + url)
              // Just like in sun/net/www/protocol/http/HttpURLConnection.java:1900
              val respCode = 500
              throw new java.io.IOException("Server returned HTTP" +
                " response code: " + respCode + " for URL: " +
                url.toString)
            }
            inputStream = Some(makeInputStream)
          }
          inputStream.get
        }

        override def connect(): Unit = {}

        override def disconnect(): Unit = ???

        override def usingProxy(): Boolean = ???
      }
  }
}

object HttpCache {
  var enabled = false

  var randomErrorEnabled = false
  private val random = new scala.util.Random

  val httpsCache = new HttpCache()
  // This method can be called at most once in a given Java Virtual Machine:
  java.net.URL.setURLStreamHandlerFactory(new java.net.URLStreamHandlerFactory() {
    override def createURLStreamHandler(protocol: String): java.net.URLStreamHandler = {
      if (protocol == "http" || protocol == "https") httpsCache else null
    }
  })
}
