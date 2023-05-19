package com.github.pflooky.datagen.core.sink.jms

import com.github.pflooky.datagen.core.model.Constants.BODY_FIELD
import com.github.pflooky.datagen.core.model.{Count, Schema, Step}
import org.apache.spark.sql.Row
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

import javax.jms.{Message, MessageProducer, Session}
import javax.naming.Context

class JmsSinkProcessorTest extends AnyFunSuite with MockFactory {

  private val step = Step("step1", "json", Count(), Map(), Schema("manual", None))

  test("Given no body field defined, push the first string column as a text message") {
    val mockSession = mock[Session]
    val mockMessageProducer = mock[MessageProducer]
    val jmsSinkProcessor = new JmsSinkProcessor(Map(), step) {
      override protected def createMessageProducer: (MessageProducer, Session) = {
        (mockMessageProducer, mockSession)
      }
    }

    val mockRow = mock[Row]
    (mockRow.getString _).expects(0).once()
    (mockSession.createTextMessage(_: String)).expects(*).once()
    (mockMessageProducer.send(_: Message)).expects(*).once()
    jmsSinkProcessor.pushRowToSink(mockRow)
  }

  test("Given a body field defined, push that string column as a text message") {
    val mockSession = mock[Session]
    val mockMessageProducer = mock[MessageProducer]
    val jmsSinkProcessor = new JmsSinkProcessor(Map(), step.copy(options = Map(BODY_FIELD -> "json_body"))) {
      override protected def createMessageProducer: (MessageProducer, Session) = {
        (mockMessageProducer, mockSession)
      }
    }

    val mockRow = mock[Row]
    (mockRow.getAs[String](_: String)).expects("json_body").once()
    (mockSession.createTextMessage(_: String)).expects(*).once()
    (mockMessageProducer.send(_: Message)).expects(*).once()
    jmsSinkProcessor.pushRowToSink(mockRow)
  }

  test("Can get all required connection properties for JMS") {
    val connectionConfig = Map(
      "initialContextFactory" -> "com.solacesystems.jndi.SolJNDIInitialContextFactory",
      "url" -> "smf://localhost:55555",
      "vpnName" -> "default",
      "user" -> "admin",
      "password" -> "admin"
    )
    val mockSession = mock[Session]
    val mockMessageProducer = mock[MessageProducer]
    val jmsSinkProcessor = new JmsSinkProcessor(connectionConfig, step) {
      override protected def createMessageProducer: (MessageProducer, Session) = {
        (mockMessageProducer, mockSession)
      }
    }

    val res = jmsSinkProcessor.getConnectionProperties
    assert(res.size() == 4)
    assert(res.containsKey(Context.INITIAL_CONTEXT_FACTORY))
    assert(res.getProperty(Context.INITIAL_CONTEXT_FACTORY) == connectionConfig("initialContextFactory"))
    assert(res.containsKey(Context.SECURITY_PRINCIPAL))
    assert(res.getProperty(Context.SECURITY_PRINCIPAL) == connectionConfig("user") + "@" + connectionConfig("vpnName"))
    assert(res.containsKey(Context.SECURITY_CREDENTIALS))
    assert(res.getProperty(Context.SECURITY_CREDENTIALS) == connectionConfig("password"))
    assert(res.containsKey(Context.PROVIDER_URL))
    assert(res.getProperty(Context.PROVIDER_URL) == connectionConfig("url"))
  }

  test("Throw exception when incomplete connection configuration provided") {
    assertThrows[RuntimeException](new JmsSinkProcessor(Map(), step))
  }

}
