package com.github.pflooky.datagen.core.sink

import com.github.pflooky.datagen.core.model.Step
import org.apache.spark.sql.Row

import java.util.concurrent.LinkedBlockingQueue

trait SinkProcessor {

  val connectionConfig: Map[String, String]
  val step: Step

  def pushRowToSink(row: Row): Unit

}

trait RealTimeSinkProcessor[T] extends SinkProcessor with Serializable {

  val connectionPool: LinkedBlockingQueue[T] = new LinkedBlockingQueue[T]()
  val maxPoolSize: Int = 5

  def init(): Unit = {
    while (connectionPool.size() < maxPoolSize) {
      connectionPool.put(createConnection)
    }
  }

  def getConnectionFromPool: T = {
    if (connectionPool.size() > 0) {
      connectionPool.take()
    } else {
      //TODO wait for a bit until another connection becomes available?
      throw new RuntimeException(s"All connection from pool are exhausted, step-name=${step.name}")
    }
  }

  def returnConnectionToPool(connection: T): Unit = {
    connectionPool.put(connection)
  }

  def createConnection: T

  def close: Unit

}
