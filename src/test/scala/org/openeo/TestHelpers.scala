package org.openeo

import scala.io.{Codec, Source}

object TestHelpers {
  private val resourcePath = "/org/openeo/"

  private[openeo] def loadJsonResource(classPathResourceName: String, codec: Codec = Codec.UTF8): String = {
    val fullPath = resourcePath + classPathResourceName
    val jsonFile = Source.fromURL(getClass.getResource(fullPath))(codec)

    try jsonFile.mkString
    finally jsonFile.close()
  }
}
