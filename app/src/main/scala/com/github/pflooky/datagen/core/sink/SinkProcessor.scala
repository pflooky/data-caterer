package com.github.pflooky.datagen.core.sink

import com.github.pflooky.datagen.core.exception.UnsupportedRealTimeDataSourceFormat
import com.github.pflooky.datagen.core.model.Constants.{HTTP, JMS}
import com.github.pflooky.datagen.core.model.Step
import com.github.pflooky.datagen.core.sink.http.HttpSinkProcessor
import com.github.pflooky.datagen.core.sink.jms.JmsSinkProcessor
import org.apache.spark.sql.Row

import java.util.concurrent.LinkedBlockingQueue

trait SinkProcessor[T] {

  var connectionConfig: Map[String, String]
  var step: Step

  def createConnection(connectionConfig: Map[String, String], step: Step): T

  def pushRowToSink(row: Row): Unit

  def close: Unit
}

trait RealTimeSinkProcessor[T] extends SinkProcessor[T] with Serializable {

  private var hasInit: Boolean = false
  lazy val connectionPool: LinkedBlockingQueue[T] = new LinkedBlockingQueue[T]()
  lazy val maxPoolSize: Int = 2

  def init(connectionConfig: Map[String, String], step: Step): Unit = {
    while (connectionPool.size() < maxPoolSize) {
      connectionPool.put(createConnection(connectionConfig, step))
    }
    hasInit = true
  }

  def createConnections(connectionConfig: Map[String, String], step: Step): SinkProcessor[_]

  def getConnectionFromPool: T = {
    if (connectionPool.size() == 0 && !hasInit) {
      init(connectionConfig, step)
    }
    connectionPool.take()
  }

  def returnConnectionToPool(connection: T): Unit = {
    connectionPool.put(connection)
  }

}

object SinkProcessor {

  def getConnection(format: String, connectionConfig: Map[String, String], step: Step): SinkProcessor[_] = {
    format match {
      case HTTP => HttpSinkProcessor.createConnections(connectionConfig, step)
      case JMS => JmsSinkProcessor.createConnections(connectionConfig, step)
      case x => throw new UnsupportedRealTimeDataSourceFormat(x)
    }
  }
}