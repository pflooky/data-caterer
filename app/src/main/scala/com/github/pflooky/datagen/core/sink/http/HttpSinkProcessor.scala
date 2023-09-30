package com.github.pflooky.datagen.core.sink.http

import com.github.pflooky.datacaterer.api.model.Step
import com.github.pflooky.datagen.core.model.Constants.{DEFAULT_HTTP_CONTENT_TYPE, DEFAULT_HTTP_METHOD, REAL_TIME_BODY_COL, REAL_TIME_CONTENT_TYPE_COL, REAL_TIME_HEADERS_COL, REAL_TIME_METHOD_COL, REAL_TIME_URL_COL}
import com.github.pflooky.datagen.core.sink.{RealTimeSinkProcessor, SinkProcessor}
import com.github.pflooky.datagen.core.util.HttpUtil.getAuthHeader
import com.github.pflooky.datagen.core.util.RowUtil.getRowValue
import io.netty.handler.codec.http.HttpHeaderNames
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.asynchttpclient.{AsyncHttpClient, Request}
import org.asynchttpclient.Dsl.asyncHttpClient

import java.nio.charset.Charset
import scala.collection.mutable
import scala.util.{Failure, Success}

object HttpSinkProcessor extends RealTimeSinkProcessor[Unit] {

  private val LOGGER = Logger.getLogger(getClass.getName)

  var connectionConfig: Map[String, String] = _
  var step: Step = _
  var http = asyncHttpClient

  override def createConnections(connectionConfig: Map[String, String], step: Step): SinkProcessor[_] = this

  override def createConnection(connectionConfig: Map[String, String], step: Step): Unit = {}

  def createConnections(connectionConfig: Map[String, String], step: Step, http: AsyncHttpClient): SinkProcessor[_] = {
    this.http = http
    this.connectionConfig = connectionConfig
    this.step = step
    this
  }

  override def close: Unit = {
    //TODO hack to wait for all connections to be finished, hard to know as connections are used across all partitions
    Thread.sleep(2000)
    http.close()
  }

  override def pushRowToSink(row: Row): Unit = {
    val request = createHttpRequest(row)
    val completableFutureResp = http.executeRequest(request).toCompletableFuture

    completableFutureResp.whenComplete((resp, error) =>
      if (error == null) {
        LOGGER.debug(s"Successful HTTP request, url=${resp.getUri}, status-code=${resp.getStatusCode}, status-text=${resp.getStatusText}, " +
          s"response-body=${resp.getResponseBody}")
      } else {
        LOGGER.error(s"Failed HTTP request, url=, message=${error.getMessage}")
      }
    )
  }

  def createHttpRequest(row: Row, connectionConfig: Option[Map[String, String]] = None): Request = {
    connectionConfig.foreach(conf => this.connectionConfig = conf)
    val httpUrl = getRowValue[String](row, REAL_TIME_URL_COL)
    val method = getRowValue[String](row, REAL_TIME_METHOD_COL, DEFAULT_HTTP_METHOD)
    val body = getRowValue[String](row, REAL_TIME_BODY_COL, "")
    val headers = getRowValue[mutable.WrappedArray[Row]](row, REAL_TIME_HEADERS_COL, mutable.WrappedArray.empty[Row])
    val contentType = getRowValue[String](row, REAL_TIME_CONTENT_TYPE_COL, DEFAULT_HTTP_CONTENT_TYPE)

    val basePrepareRequest = http.prepare(method, httpUrl)
      .setBody(body)

    (getHeaders(headers) ++ Map(HttpHeaderNames.CONTENT_TYPE -> contentType))
      .foldLeft(basePrepareRequest)((req, header) => req.addHeader(header._1, header._2))
    basePrepareRequest.build()
  }

  private def getHeaders(rowHeaders: mutable.WrappedArray[Row]): Map[String, String] = {
    val baseHeaders = rowHeaders.map(r => (r.getAs[String]("key"), r.getAs[String]("value"))).toMap
    baseHeaders ++ getAuthHeader(connectionConfig)
  }
}
