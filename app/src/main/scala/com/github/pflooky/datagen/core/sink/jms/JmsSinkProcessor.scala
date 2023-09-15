package com.github.pflooky.datagen.core.sink.jms

import com.github.pflooky.datacaterer.api.model.Constants.{JMS_CONNECTION_FACTORY, JMS_DESTINATION_NAME, JMS_INITIAL_CONTEXT_FACTORY, JMS_VPN_NAME, PASSWORD, URL, USERNAME}
import com.github.pflooky.datacaterer.api.model.Step
import com.github.pflooky.datagen.core.model.Constants.{REAL_TIME_BODY_COL, REAL_TIME_HEADERS_COL, REAL_TIME_PARTITION_COL}
import com.github.pflooky.datagen.core.sink.{RealTimeSinkProcessor, SinkProcessor}
import com.github.pflooky.datagen.core.util.RowUtil.getRowValue
import org.apache.hadoop.shaded.com.nimbusds.jose.util.StandardCharset
import org.apache.spark.sql.Row

import java.util.Properties
import javax.jms.{Connection, ConnectionFactory, Destination, MessageProducer, Session, TextMessage}
import javax.naming.{Context, InitialContext}


object JmsSinkProcessor extends RealTimeSinkProcessor[(MessageProducer, Session, Connection)] {

  var connectionConfig: Map[String, String] = _
  var step: Step = _

  def pushRowToSink(row: Row): Unit = {
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

  override def createConnections(connectionConfig: Map[String, String], step: Step): SinkProcessor[_] = {
    this.connectionConfig = connectionConfig
    this.step = step
    init(connectionConfig, step)
    this
  }

  def createConnections(messageProducer: MessageProducer, session: Session, connection: Connection, step: Step): SinkProcessor[_] = {
    connectionPool.clear()
    connectionPool.put((messageProducer, session, connection))
    this.step = step
    this
  }

  def createConnection(connectionConfig: Map[String, String], step: Step): (MessageProducer, Session, Connection) = {
    val (connection, context) = createInitialConnection(connectionConfig)
    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val destination = context.lookup(s"${step.options(JMS_DESTINATION_NAME)}").asInstanceOf[Destination]
    val messageProducer = session.createProducer(destination)
    (messageProducer, session, connection)
  }

  def close: Unit = {
    //TODO hack to wait for all producers to be finished, hard to know as connections are used across all partitions
    Thread.sleep(1000)
    while (connectionPool.size() > 0) {
      val (messageProducer, session, connection) = connectionPool.take()
      messageProducer.close()
      connection.close()
      session.close()
    }
  }

  protected def createInitialConnection(connectionConfig: Map[String, String]): (Connection, InitialContext) = {
    val properties: Properties = getConnectionProperties(connectionConfig)
    val context = new InitialContext(properties)
    val cf = context.lookup(connectionConfig(JMS_CONNECTION_FACTORY)).asInstanceOf[ConnectionFactory]
    (cf.createConnection(), context)
  }

  def getConnectionProperties(connectionConfig: Map[String, String]): Properties = {
    val properties = new Properties()
    properties.put(Context.INITIAL_CONTEXT_FACTORY, connectionConfig(JMS_INITIAL_CONTEXT_FACTORY))
    properties.put(Context.PROVIDER_URL, connectionConfig(URL))
    properties.put(Context.SECURITY_PRINCIPAL, s"${connectionConfig(USERNAME)}@${connectionConfig(JMS_VPN_NAME)}")
    properties.put(Context.SECURITY_CREDENTIALS, connectionConfig(PASSWORD))
    properties
  }
}