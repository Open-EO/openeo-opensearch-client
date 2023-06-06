package org.openeo

import java.io.{File, FileInputStream, InputStream}
import java.net.URL
import java.nio.file.{Files, Paths}

class HttpCache extends sun.net.www.protocol.https.Handler {

  def openConnectionSuper(url: URL): java.net.URLConnection = super.openConnection(url)

  override def openConnection(url: URL): java.net.URLConnection =
    if (!HttpCache.enabled) {
      openConnectionSuper(url)
    } else if (url.toString.contains(".tif")) {
      // Tiff could be requested partially, and this proxy does not support that
      openConnectionSuper(url)
    } else
      new java.net.HttpURLConnection(url) {
        println("HttpCache.openConnection()")
        private lazy val inputStream = {
          val fullUrl = this.url.toString
          val idx = fullUrl.indexOf("//")
          var filePath = fullUrl.substring(idx + 2)
          if (filePath.length > 255) {
            // An individual name should be max 255 characters long. Lazy implementation caps whole file path:
            val hash = "___" + (filePath.hashCode >>> 1).toString // TODO, parse extension?
            filePath = filePath.substring(0, 255 - hash.length) + hash
          }
          val lastSlash = filePath.lastIndexOf("/")
          val (basePath, filename) = filePath.splitAt(lastSlash + 1)
          filePath = basePath + filename

          val cachePath = getClass.getResource("/org/openeo/httpsCache").getPath
          // val cachePath = "src/test/resources/org/openeo/httpsCache" // Use this to cache files to git.
          val path = Paths.get(cachePath, filePath)
          if (!Files.exists(path)) {
            println("Caching request url: " + url)
            Files.createDirectories(Paths.get(cachePath, basePath))
            val stream = openConnectionSuper(url).getInputStream
            val tmpBeforeAtomicMove = Paths.get(cachePath, java.util.UUID.randomUUID().toString)
            Files.copy(stream, tmpBeforeAtomicMove)
            Files.move(tmpBeforeAtomicMove, path)
          } else {
            println("Using cached request: " + path.toUri)
            println("Using cached request url: " + url)
          }
          new FileInputStream(new File(path.toString))
        }

        override def getInputStream: InputStream = inputStream

        override def connect(): Unit = {}

        override def disconnect(): Unit = ???

        override def usingProxy(): Boolean = ???
      }
}

object HttpCache {
  var enabled = false

  val httpsCache = new HttpCache()
  // This method can be called at most once in a given Java Virtual Machine:
  java.net.URL.setURLStreamHandlerFactory(new java.net.URLStreamHandlerFactory() {
    override def createURLStreamHandler(protocol: String): java.net.URLStreamHandler = {
      println("createURLStreamHandler")
      if (protocol == "http" || protocol == "https")
        if (enabled)
          httpsCache
        else null
      else null
    }
  })
}
