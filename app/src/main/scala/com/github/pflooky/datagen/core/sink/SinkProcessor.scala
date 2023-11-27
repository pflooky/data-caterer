package com.github.pflooky.datagen.core.sink

import com.github.pflooky.datacaterer.api.model.Step
import org.apache.spark.sql.Row

trait SinkProcessor[T] {

  var connectionConfig: Map[String, String]
  var step: Step

  def createConnection(connectionConfig: Map[String, String], step: Step): T

  def pushRowToSink(row: Row): Unit

  def close: Unit
}
