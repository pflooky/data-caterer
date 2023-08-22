package com.github.pflooky.datagen.core.sink.http

import com.github.pflooky.datagen.core.model.Constants.{PASSWORD, REAL_TIME_BODY_COL, REAL_TIME_CONTENT_TYPE_COL, REAL_TIME_HEADERS_COL, REAL_TIME_METHOD_COL, REAL_TIME_URL_COL, USERNAME}
import com.github.pflooky.datagen.core.model.Step
import com.github.pflooky.datagen.core.sink.{RealTimeSinkProcessor, SinkProcessor}
import com.github.pflooky.datagen.core.util.RowUtil.getRowValue
import dispatch.Defaults._
import dispatch._
import org.apache.log4j.Logger
import org.apache.spark.sql.Row

import java.nio.charset.Charset
import java.util.Base64
import scala.collection.mutable
import scala.util.{Failure, Success}

object HttpSinkProcessor extends RealTimeSinkProcessor[Unit] {

  private val LOGGER = Logger.getLogger(getClass.getName)

  var connectionConfig: Map[String, String] = _
  var step: Step = _
  var http: Http = Http.default

  override def createConnections(connectionConfig: Map[String, String], step: Step): SinkProcessor[_] = this
  override def createConnection(connectionConfig: Map[String, String], step: Step): Unit = {}

  def createConnections(connectionConfig: Map[String, String], step: Step, http: Http): SinkProcessor[_] = {
    this.http = http
    this
  }

  override def close: Unit = {
    //TODO hack to wait for all connections to be finished, hard to know as connections are used across all partitions
    Thread.sleep(2000)
    http.client.close()
  }

  override def pushRowToSink(row: Row): Unit = {
    val request = createHttpRequest(row)
    val resp = http(request)

    resp.onComplete {
      case Success(value) =>
        LOGGER.debug(s"Successful HTTP request, url=${value.getUri}, status-code=${value.getStatusCode}, status-text=${value.getStatusText}, " +
          s"response-body=${value.getResponseBody}")
      case Failure(exception) =>
        LOGGER.error(s"Failed HTTP request, url=, message=${exception.getMessage}")
    }
  }

  def createHttpRequest(row: Row): Req = {
    val httpUrl = getRowValue[String](row, REAL_TIME_URL_COL)
    val method = getRowValue[String](row, REAL_TIME_METHOD_COL, "GET")
    val body = getRowValue[String](row, REAL_TIME_BODY_COL, "")
    val headers = getRowValue[mutable.WrappedArray[Row]](row, REAL_TIME_HEADERS_COL, mutable.WrappedArray.empty[Row])
    val contentType = getRowValue[String](row, REAL_TIME_CONTENT_TYPE_COL, "application/json")

    url(httpUrl)
      .setMethod(method)
      .setBody(body)
      .setHeaders(getHeaders(headers))
      .setContentType(contentType, Charset.defaultCharset())
  }

  private def getHeaders(rowHeaders: mutable.WrappedArray[Row]): Map[String, Seq[String]] = {
    val baseHeaders = rowHeaders.map(r => (r.getAs[String]("key"), Seq(r.getAs[String]("value")))).toMap
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
