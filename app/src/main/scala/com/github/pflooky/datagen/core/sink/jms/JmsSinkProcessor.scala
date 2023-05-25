package com.github.pflooky.datagen.core.sink.jms

import com.github.pflooky.datagen.core.model.Constants.{BODY_FIELD, JMS_CONNECTION_FACTORY, JMS_DESTINATION_NAME, JMS_INITIAL_CONTEXT_FACTORY, JMS_VPN_NAME, PASSWORD, URL, USERNAME}
import com.github.pflooky.datagen.core.model.Step
import com.github.pflooky.datagen.core.sink.SinkProcessor
import org.apache.spark.sql.Row

import java.util.Properties
import javax.jms.{Connection, ConnectionFactory, Destination, MessageProducer, Session}
import javax.naming.{Context, InitialContext}

class JmsSinkProcessor(override val connectionConfig: Map[String, String],
                       override val step: Step) extends SinkProcessor {

  private val (messageProducer, session) = createMessageProducer
  private val bodyFieldOpt = step.options.get(BODY_FIELD)

  override def pushRowToSink(row: Row): Unit = {
    val body = bodyFieldOpt.map(row.getAs[String]).getOrElse(row.json)
    val message = session.createTextMessage(body)
    messageProducer.send(message)
  }

  protected def createConnection: (Connection, InitialContext) = {
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

  protected def createMessageProducer: (MessageProducer, Session) = {
    val (connection, context) = createConnection
    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val destination = context.lookup(s"${step.options(JMS_DESTINATION_NAME)}").asInstanceOf[Destination]
    (session.createProducer(destination), session)
  }
}
