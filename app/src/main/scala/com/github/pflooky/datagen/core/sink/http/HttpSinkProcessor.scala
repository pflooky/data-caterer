package com.github.pflooky.datagen.core.sink.http

import com.github.pflooky.datagen.core.model.Constants.{BODY_FIELD, HTTP_CONTENT_TYPE, HTTP_HEADER_PREFIX, HTTP_METHOD, PASSWORD, URL, USERNAME}
import com.github.pflooky.datagen.core.model.Step
import com.github.pflooky.datagen.core.sink.{RealTimeSinkProcessor, SinkProcessor}
import dispatch.Defaults._
import dispatch._
import org.apache.log4j.Logger
import org.apache.spark.sql.Row

import java.nio.charset.Charset
import java.util.Base64
import scala.util.{Failure, Success}

class HttpSinkProcessor(override val connectionConfig: Map[String, String],
                        override val step: Step,
                        http: Http = Http.default) extends RealTimeSinkProcessor[Unit] {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val METHOD = step.options(HTTP_METHOD)
  private val HTTP_URL = connectionConfig(URL)
  private val BODY_FIELD_OPT = step.options.get(BODY_FIELD)
  private val CONTENT_TYPE = step.options.getOrElse(HTTP_CONTENT_TYPE, "application/json")
  private val HEADERS = getHeaders

  override def createConnection: Unit = {}

  override def close: Unit = {}

  override def pushRowToSink(row: Row): Unit = {
    val request = createHttpRequest(row)
    val resp = http(request)

    resp.onComplete {
      case Success(value) =>
        LOGGER.debug(s"Successful HTTP request, url=${value.getUri}, status-code=${value.getStatusCode}, status-text=${value.getStatusText}, " +
          s"response-body=${value.getResponseBody}")
      case Failure(exception) =>
        LOGGER.error(s"Failed HTTP request, url=$HTTP_URL, message=${exception.getMessage}")
    }
  }

  def createHttpRequest(row: Row): Req = {
    val body = BODY_FIELD_OPT.map(row.getAs[String]).getOrElse(row.json)

    url(HTTP_URL)
      .setMethod(METHOD)
      .setBody(body)
      .setHeaders(HEADERS)
      .setContentType(CONTENT_TYPE, Charset.defaultCharset())
  }

  protected def getHeaders: Map[String, Seq[String]] = {
    val baseHeaders = step.options.filter(_._1.startsWith(s"$HTTP_HEADER_PREFIX."))
      .map(h => {
        val key = h._1.replaceFirst(HTTP_HEADER_PREFIX + "\\.", "")
        (key, Seq(h._2))
      })
    if (connectionConfig.contains(USERNAME) && connectionConfig.contains(PASSWORD)) {
      val user = connectionConfig(USERNAME)
      val password = connectionConfig(PASSWORD)
      val encodedUserPassword = Base64.getEncoder.encodeToString(s"$user:$password".getBytes)
      baseHeaders ++ Map("Authorization" -> Seq(s"Basic $encodedUserPassword"))
    } else {
      baseHeaders
    }
  }
}
