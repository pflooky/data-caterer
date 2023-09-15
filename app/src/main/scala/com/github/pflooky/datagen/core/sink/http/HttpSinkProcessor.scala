package com.github.pflooky.datagen.core.sink.http

import com.github.pflooky.datacaterer.api.model.Step
import com.github.pflooky.datagen.core.model.Constants.{DEFAULT_HTTP_CONTENT_TYPE, DEFAULT_HTTP_METHOD, REAL_TIME_BODY_COL, REAL_TIME_CONTENT_TYPE_COL, REAL_TIME_HEADERS_COL, REAL_TIME_METHOD_COL, REAL_TIME_URL_COL}
import com.github.pflooky.datagen.core.sink.{RealTimeSinkProcessor, SinkProcessor}
import com.github.pflooky.datagen.core.util.HttpUtil.getAuthHeader
import com.github.pflooky.datagen.core.util.RowUtil.getRowValue
import dispatch.Defaults._
import dispatch._
import org.apache.log4j.Logger
import org.apache.spark.sql.Row

import java.nio.charset.Charset
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
    this.connectionConfig = connectionConfig
    this.step = step
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

  def createHttpRequest(row: Row, connectionConfig: Option[Map[String, String]] = None): Req = {
    connectionConfig.foreach(conf => this.connectionConfig = conf)
    val httpUrl = getRowValue[String](row, REAL_TIME_URL_COL)
    val method = getRowValue[String](row, REAL_TIME_METHOD_COL, DEFAULT_HTTP_METHOD)
    val body = getRowValue[String](row, REAL_TIME_BODY_COL, "")
    val headers = getRowValue[mutable.WrappedArray[Row]](row, REAL_TIME_HEADERS_COL, mutable.WrappedArray.empty[Row])
    val contentType = getRowValue[String](row, REAL_TIME_CONTENT_TYPE_COL, DEFAULT_HTTP_CONTENT_TYPE)

    url(httpUrl)
      .setMethod(method)
      .setBody(body)
      .setHeaders(getHeaders(headers))
      .setContentType(contentType, Charset.defaultCharset())
  }

  private def getHeaders(rowHeaders: mutable.WrappedArray[Row]): Map[String, Seq[String]] = {
    val baseHeaders = rowHeaders.map(r => (r.getAs[String]("key"), Seq(r.getAs[String]("value")))).toMap
    baseHeaders ++ getAuthHeader(connectionConfig)
  }
}
