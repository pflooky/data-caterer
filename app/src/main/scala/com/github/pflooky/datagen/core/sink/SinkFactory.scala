package com.github.pflooky.datagen.core.sink

import com.github.pflooky.datagen.core.exception.UnsupportedRealTimeDataSourceFormat
import com.github.pflooky.datagen.core.model.Constants.{BATCH, FAILED, FINISHED, FORMAT, HTTP, JMS, JSON, REAL_TIME, SAVE_MODE, STARTED}
import com.github.pflooky.datagen.core.model.{Step, Task, TaskSummary}
import com.github.pflooky.datagen.core.sink.http.HttpSinkProcessor
import com.github.pflooky.datagen.core.sink.jms.JmsSinkProcessor
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

class SinkFactory(
                   val connectionConfigs: Map[String, Map[String, String]]
                 )(implicit val sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def pushToSink(df: DataFrame, dataSourceName: String, step: Step, enableCount: Boolean): Unit = {
    if (!connectionConfigs.contains(dataSourceName)) {
      throw new RuntimeException(s"Cannot find sink connection details in application.conf for data source: $dataSourceName")
    }
    val connectionConfig = connectionConfigs(dataSourceName)
    val saveMode = connectionConfig.get(SAVE_MODE).map(_.toLowerCase.capitalize).map(SaveMode.valueOf).getOrElse(SaveMode.Append)
    val saveModeName = saveMode.name()
    val format = connectionConfig(FORMAT)
    df.cache()
    val count = if (enableCount) {
      df.count().toString
    } else {
      LOGGER.warn("Count is disabled. It will help with performance. Defaulting to -1")
      "-1"
    }
    LOGGER.info(s"Pushing data to sink, data-source-name=$dataSourceName, save-mode=$saveModeName, num-records=$count, status=$STARTED")
    val saveTiming = determineSaveTiming(dataSourceName, format)
    val trySaveData = Try(if (saveTiming == BATCH) {
      saveBatchData(df, saveMode, connectionConfig, step.options)
    } else {
      saveRealTimeData(df, format, connectionConfig, step)
    })

    trySaveData match {
      case Failure(exception) =>
        throw new RuntimeException(s"Failed to save data for sink, data-source-name=$dataSourceName, save-mode=$saveModeName, num-records=$count, status=$FAILED", exception)
      case Success(_) =>
        LOGGER.info(s"Successfully saved data to sink, data-source-name=$dataSourceName, save-mode=$saveModeName, num-records=$count, status=$FINISHED")
    }
  }

  private def determineSaveTiming(dataSourceName: String, format: String): String = {
    format match {
      case HTTP | JMS =>
        LOGGER.info(s"Given the step type is either HTTP or JMS, data will be generated in real-time mode. " +
          s"It will be based on requests per second defined at plan level, data-source-name=$dataSourceName, format=$format")
        REAL_TIME
      case _ =>
        LOGGER.info(s"Will generate data in batch mode for step, data-source-name=$dataSourceName, format=$format")
        BATCH
    }
  }

  private def saveBatchData(df: DataFrame, saveMode: SaveMode, connectionConfig: Map[String, String], stepOptions: Map[String, String]): Unit = {
    df.write
      .format(connectionConfig(FORMAT))
      .mode(saveMode)
      .options(connectionConfig)
      .options(stepOptions)
      .save()
  }

  private def saveRealTimeData(df: DataFrame, format: String, connectionConfig: Map[String, String], step: Step): Unit = {
    val bodyData = if (step.`type`.toLowerCase == JSON) {
      df.selectExpr("to_json()")
    } else df
    df.rdd.foreachPartition(partition => {
      val sinkProcessor = format match {
        case HTTP => new HttpSinkProcessor(connectionConfig, step)
        case JMS => new JmsSinkProcessor(connectionConfig, step)
        case x => throw new UnsupportedRealTimeDataSourceFormat(x)
      }
      partition.foreach(r => sinkProcessor.pushRowToSink(r))
    })
  }
}
