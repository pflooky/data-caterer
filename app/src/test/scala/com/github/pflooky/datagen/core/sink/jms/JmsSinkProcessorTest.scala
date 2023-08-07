package com.github.pflooky.datagen.core.sink.jms

import com.github.pflooky.datagen.core.model.Constants.{REAL_TIME_BODY_COL, REAL_TIME_HEADERS_COL, REAL_TIME_PARTITION_COL, REAL_TIME_URL_COL}
import com.github.pflooky.datagen.core.model.{Count, Field, Schema, Step}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalamock.util.Defaultable
import org.scalatest.funsuite.AnyFunSuite

import javax.jms.{Connection, Message, MessageProducer, Session, TextMessage}
import javax.naming.Context

class JmsSinkProcessorTest extends AnyFunSuite with MockFactory {

  private val mockConnection = mock[Connection]
  private val basicFields = List(Field(REAL_TIME_BODY_COL))
  private val step = Step("step1", "json", Count(), Map(), Schema("manual", Some(basicFields)))
  implicit val d = new Defaultable[java.util.Enumeration[_]] {
    override val default = null
  }

  test("Push value as a basic text message") {
    val mockSession = mock[Session]
    val mockMessageProducer = mock[MessageProducer]
    val jmsSinkProcessor = new JmsSinkProcessor(Map(), step) {
      override def createConnection: (MessageProducer, Session, Connection) = {
        (mockMessageProducer, mockSession, mockConnection)
      }
    }

    val mockRow = mock[Row]
    val mockMessage = mock[TestTextMessage]
    (mockRow.schema _).expects().anyNumberOfTimes().returns(StructType(Seq(StructField(REAL_TIME_URL_COL, StringType))))
    (mockRow.getAs[String](_: String)).expects(REAL_TIME_BODY_COL).once()
    (mockSession.createTextMessage(_: String)).expects(*).once().returns(mockMessage)
    (mockMessage.setJMSPriority(_: Int)).expects(4).once()
    (mockMessageProducer.send(_: Message)).expects(*).once()
    jmsSinkProcessor.pushRowToSink(mockRow)
  }

  test("Given a partition field defined, set the message priority based on the partition field value") {
    val fields = basicFields ++ List(Field(REAL_TIME_PARTITION_COL))
    val schema = Schema("manual", Some(fields))
    val mockSession = mock[Session]
    val mockMessageProducer = mock[MessageProducer]
    val jmsSinkProcessor = new JmsSinkProcessor(Map(), step.copy(schema = schema)) {
      override def createConnection: (MessageProducer, Session, Connection) = {
        (mockMessageProducer, mockSession, mockConnection)
      }
    }

    val mockRow = mock[Row]
    val mockMessage = mock[TestTextMessage]
    (mockRow.schema _).expects().anyNumberOfTimes().returns(StructType(Seq(StructField(REAL_TIME_URL_COL, StringType), StructField(REAL_TIME_PARTITION_COL, StringType))))
    (mockSession.createTextMessage(_: String)).expects(*).once().returns(mockMessage)
    (mockRow.getAs[String](_: String)).expects(REAL_TIME_BODY_COL).once()
    (mockRow.getAs[Int](_: String)).expects(REAL_TIME_PARTITION_COL).once().returns(1)
    (mockMessage.setJMSPriority(_: Int)).expects(1).once()
    (mockMessageProducer.send(_: Message)).expects(*).once()
    jmsSinkProcessor.pushRowToSink(mockRow)
  }

  test("Given a headers field defined, set the message properties") {
    val fields = basicFields ++ List(Field(REAL_TIME_HEADERS_COL))
    val schema = Schema("manual", Some(fields))
    val mockSession = mock[Session]
    val mockMessageProducer = mock[MessageProducer]
    val jmsSinkProcessor = new JmsSinkProcessor(Map(), step.copy(schema = schema)) {
      override def createConnection: (MessageProducer, Session, Connection) = {
        (mockMessageProducer, mockSession, mockConnection)
      }
    }

    val mockRow = mock[Row]
    val mockMessage = mock[TestTextMessage]
    (mockRow.schema _).expects().anyNumberOfTimes().returns(StructType(Seq(
      StructField(REAL_TIME_URL_COL, StringType),
      StructField(REAL_TIME_HEADERS_COL, ArrayType(StructType(Seq(
        StructField("key", StringType),
        StructField("value", StringType)
      ))))
    )))
    (mockSession.createTextMessage(_: String)).expects(*).once().returns(mockMessage)
    (mockRow.getAs[String](_: String)).expects(REAL_TIME_BODY_COL).once()
    (mockRow.getAs[Array[(String, Array[Byte])]](_: String)).expects(REAL_TIME_HEADERS_COL).once().returns(Array(("account-id", "abc123".getBytes)))
    (mockMessage.setStringProperty(_: String, _: String)).expects("account-id", "abc123").once()
    (mockMessage.setJMSPriority(_: Int)).expects(4).once()
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
      override def createConnection: (MessageProducer, Session, Connection) = {
        (mockMessageProducer, mockSession, mockConnection)
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
    assertThrows[RuntimeException](new JmsSinkProcessor(Map(), step).createConnection)
  }

}

//reference: https://github.com/paulbutcher/ScalaMock/issues/86
trait TestTextMessage extends TextMessage {
  override def isBodyAssignableTo(c: Class[_]): Boolean = this.isBodyAssignableTo(c)
}
