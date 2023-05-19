package com.github.pflooky.datagen.core.sink.http

import com.github.pflooky.datagen.core.model.Constants.{BODY_FIELD, HTTP_METHOD, PASSWORD, URL, USERNAME}
import com.github.pflooky.datagen.core.model.{Count, Schema, Step}
import dispatch.{Future, Http, Req}
import org.apache.spark.sql.Row
import org.asynchttpclient.Response
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.ExecutionContext

class HttpSinkProcessorTest extends AnyFunSuite with MockFactory {

  test("Can send HTTP POST request with body and headers") {
    val stepOptions = Map(
      HTTP_METHOD -> "POST",
      BODY_FIELD -> "json_body",
      "httpHeader.id" -> "id123",
      "httpHeader.type" -> "account"
    )
    val step = Step("step1", "json", Count(), stepOptions, Schema("manual", None))
    val connectionConfig = Map(
      URL -> "http://localhost:8080/customer",
      USERNAME -> "admin",
      PASSWORD -> "password"
    )
    val httpSinkProcessor = new HttpSinkProcessor(connectionConfig, step)
    val mockRow = mock[Row]
    (mockRow.getAs[String](_: String)).expects("json_body").once()

    val res = httpSinkProcessor.createHttpRequest(mockRow).toRequest

    assert(res.getUrl == connectionConfig(URL))
    assert(res.getMethod == "POST")
    val headers = res.getHeaders
    assert(headers.size() == 4)
    assert(headers.get("id") == "id123")
    assert(headers.get("type") == "account")
    assert(headers.get("Authorization") == "Basic YWRtaW46cGFzc3dvcmQ=")
    assert(headers.get("Content-Type") == "application/json; charset=UTF-8")
  }

  test("Can push data to HTTP endpoint") {
    val mockHttp = mock[Http]
    val mockRow = mock[Row]
    val connectionConfig = Map(URL -> "http://localhost:8080/help")
    val stepOptions = Map(HTTP_METHOD -> "GET")
    val step = Step("step1", "json", Count(), stepOptions, Schema("manual", None))
    val httpSinkProcessor = new HttpSinkProcessor(connectionConfig, step, mockHttp)
    val mockResp = mock[Response]
    val futureResp = Future.successful(mockResp)
    (mockHttp.apply(_: Req)(_: ExecutionContext)).expects(*, *).returns(futureResp).once()
    (mockRow.getString _).expects(0).once()
    httpSinkProcessor.pushRowToSink(mockRow)
  }

  test("Still able to proceed even when exception occurs during HTTP call") {
    val mockHttp = mock[Http]
    val mockRow = mock[Row]
    val connectionConfig = Map(URL -> "http://localhost:8080/help")
    val stepOptions = Map(HTTP_METHOD -> "GET")
    val step = Step("step1", "json", Count(), stepOptions, Schema("manual", None))
    val httpSinkProcessor = new HttpSinkProcessor(connectionConfig, step, mockHttp)
    val futureResp = Future.failed(new Throwable())
    (mockHttp.apply(_: Req)(_: ExecutionContext)).expects(*, *).returns(futureResp).once()
    (mockRow.getString _).expects(0).once()
    httpSinkProcessor.pushRowToSink(mockRow)
  }
}
