package com.github.pflooky.datagen.core.sink.http

import com.github.pflooky.datacaterer.api.model.Step
import com.github.pflooky.datagen.core.model.Constants.{DEFAULT_HTTP_CONTENT_TYPE, DEFAULT_HTTP_METHOD, REAL_TIME_BODY_COL, REAL_TIME_CONTENT_TYPE_COL, REAL_TIME_HEADERS_COL, REAL_TIME_METHOD_COL, REAL_TIME_URL_COL}
import com.github.pflooky.datagen.core.sink.{RealTimeSinkProcessor, SinkProcessor}
import com.github.pflooky.datagen.core.util.HttpUtil.getAuthHeader
import com.github.pflooky.datagen.core.util.RowUtil.getRowValue
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.ssl.util.TrustManagerFactoryWrapper
import io.netty.handler.ssl.{SslContext, SslContextBuilder}
import org.apache.http.ssl.{SSLContexts, TrustStrategy}
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.asynchttpclient.{AsyncHttpClient, AsyncHttpClientConfig, DefaultAsyncHttpClientConfig, Request, Response}
import org.asynchttpclient.Dsl.asyncHttpClient

import java.nio.charset.{Charset, StandardCharsets}
import java.security.cert.X509Certificate
import java.util.concurrent.CompletableFuture
import javax.net.ssl.{TrustManager, X509TrustManager}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object HttpSinkProcessor extends RealTimeSinkProcessor[Unit] with Serializable {

  private val LOGGER = Logger.getLogger(getClass.getName)

  var connectionConfig: Map[String, String] = _
  var step: Step = _
  var http: AsyncHttpClient = buildClient

  override def createConnections(connectionConfig: Map[String, String], step: Step): SinkProcessor[_] = {
    this.connectionConfig = connectionConfig
    this.step = step
    this
  }

  override def createConnection(connectionConfig: Map[String, String], step: Step): Unit = {}

  def createConnections(connectionConfig: Map[String, String], step: Step, http: AsyncHttpClient): SinkProcessor[_] = {
    this.http = http
    this.connectionConfig = connectionConfig
    this.step = step
    this
  }

  override def close: Unit = {
    Thread.sleep(1000)
    val activeConnections = http.getClientStats.getTotalActiveConnectionCount
    val idleConnections = http.getClientStats.getTotalIdleConnectionCount
    if (activeConnections == 0 && idleConnections == 0) {
      http.close()
    } else {
      close
    }
  }

  override def pushRowToSink(row: Row): Unit = {
    pushRowToSinkFuture(row)
  }

  private def pushRowToSinkFuture(row: Row): CompletableFuture[Response] = {
    if (http.isClosed) {
      http = buildClient
    }
    val request = createHttpRequest(row)
    val completableFutureResp = http.executeRequest(request).toCompletableFuture

    completableFutureResp.whenComplete((resp, error) =>
      if (error == null) {
        LOGGER.debug(s"Successful HTTP request, url=${resp.getUri}, status-code=${resp.getStatusCode}, status-text=${resp.getStatusText}, " +
          s"response-body=${resp.getResponseBody}")
        //TODO can save response body along with request in file for validations
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

    (getHeaders(headers) ++ Map("content-type" -> contentType))
      .foldLeft(basePrepareRequest)((req, header) => {
        val tryAddHeader = Try(req.addHeader(header._1, header._2))
        tryAddHeader match {
          case Failure(exception) =>
            val message = s"Failed to add header to HTTP request, exception=$exception"
            LOGGER.error(message)
            throw new RuntimeException(message, exception)
          case Success(value) => value
        }
      })
    basePrepareRequest.build()
  }

  private def getHeaders(rowHeaders: mutable.WrappedArray[Row]): Map[String, String] = {
    val baseHeaders = rowHeaders.map(r => (
      r.getAs[String]("key"),
      r.getAs[String]("value")
    )).toMap
    baseHeaders ++ getAuthHeader(connectionConfig)
  }

  private def buildClient: AsyncHttpClient = {
    val trustManager = new X509TrustManager {
      override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = {}

      override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = {}

      override def getAcceptedIssuers: Array[X509Certificate] = null
    }
    val sslContext = SslContextBuilder.forClient().trustManager(trustManager).build()
    val config = new DefaultAsyncHttpClientConfig.Builder().setSslContext(sslContext)
    asyncHttpClient
  }
}
