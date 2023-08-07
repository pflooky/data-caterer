package com.github.pflooky.datagen.core.sink.jms

import com.github.pflooky.datagen.core.model.Constants.{JMS_CONNECTION_FACTORY, JMS_DESTINATION_NAME, JMS_INITIAL_CONTEXT_FACTORY, JMS_VPN_NAME, PASSWORD, REAL_TIME_BODY_COL, REAL_TIME_HEADERS_COL, REAL_TIME_PARTITION_COL, URL, USERNAME}
import com.github.pflooky.datagen.core.model.Step
import com.github.pflooky.datagen.core.sink.RealTimeSinkProcessor
import com.github.pflooky.datagen.core.util.RowUtil.getRowValue
import org.apache.hadoop.shaded.com.nimbusds.jose.util.StandardCharset
import org.apache.spark.sql.Row

import java.util.Properties
import javax.jms.{Connection, ConnectionFactory, Destination, MessageProducer, Session, TextMessage}
import javax.naming.{Context, InitialContext}


class JmsSinkProcessor(override var connectionConfig: Map[String, String],
                       override var step: Step) extends RealTimeSinkProcessor[(MessageProducer, Session, Connection)] {
  override val maxPoolSize: Int = 1

  override def pushRowToSink(row: Row): Unit = {
    val body = row.getAs[String](REAL_TIME_BODY_COL)
    val (messageProducer, session, connection) = getConnectionFromPool
    val message = session.createTextMessage(body)
    setAdditionalMessageProperties(row, message)
    messageProducer.send(message)
    returnConnectionToPool((messageProducer, session, connection))
  }

  private def setAdditionalMessageProperties(row: Row, message: TextMessage): Unit = {
    val jmsPriority = getRowValue(row, REAL_TIME_PARTITION_COL, 4)
    message.setJMSPriority(jmsPriority)
    val properties = getRowValue[Array[(String, Array[Byte])]](row, REAL_TIME_HEADERS_COL, Array())
    properties.foreach(property => message.setStringProperty(property._1, new String(property._2, StandardCharset.UTF_8)))
  }

  override def createConnection: (MessageProducer, Session, Connection) = {
    val (connection, context) = createInitialConnection
    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val destination = context.lookup(s"${step.options(JMS_DESTINATION_NAME)}").asInstanceOf[Destination]
    val messageProducer = session.createProducer(destination)
    (messageProducer, session, connection)
  }

  override def close: Unit = {
    while (connectionPool.size() > 0) {
      val (messageProducer, session, connection) = connectionPool.take()
      messageProducer.close()
      connection.close()
      session.close()
    }
  }

  protected def createInitialConnection: (Connection, InitialContext) = {
    val properties: Properties = getConnectionProperties
    val context = new InitialContext(properties)
    val cf = context.lookup(connectionConfig(JMS_CONNECTION_FACTORY)).asInstanceOf[ConnectionFactory]
    (cf.createConnection(), context)
  }

  def getConnectionProperties: Properties = {
    val properties = new Properties()
    properties.put(Context.INITIAL_CONTEXT_FACTORY, connectionConfig(JMS_INITIAL_CONTEXT_FACTORY))
    properties.put(Context.PROVIDER_URL, connectionConfig(URL))
    properties.put(Context.SECURITY_PRINCIPAL, s"${connectionConfig(USERNAME)}@${connectionConfig(JMS_VPN_NAME)}")
    properties.put(Context.SECURITY_CREDENTIALS, connectionConfig(PASSWORD))
    properties
  }
}
