package com.github.pflooky.datagen.core.sink

import com.github.pflooky.datagen.core.model.Step
import org.apache.spark.sql.Row

trait SinkProcessor {

  val connectionConfig: Map[String, String]
  val step: Step

  def pushRowToSink(row: Row): Unit

}
