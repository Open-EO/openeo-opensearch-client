package org.openeo.opensearch

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8

/** Minimal immutable GET request description: a base URL plus query parameters. */
case class HttpRequest(baseUrl: String, queryParams: Seq[(String, String)] = Seq()) {
  def param(key: String, value: String): HttpRequest = copy(queryParams = queryParams :+ (key -> value))

  def params(additional: Iterable[(String, String)]): HttpRequest = copy(queryParams = queryParams ++ additional)

  /** The full URL including the URL-encoded query parameters. */
  def url: String = {
    if (queryParams.isEmpty) baseUrl
    else {
      val query = queryParams
        .map { case (k, v) => s"${URLEncoder.encode(k, UTF_8)}=${URLEncoder.encode(v, UTF_8)}" }
        .mkString("&")
      val separator = if (baseUrl.contains("?")) "&" else "?"
      s"$baseUrl$separator$query"
    }
  }
}
