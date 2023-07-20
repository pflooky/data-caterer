package com.github.pflooky.datagen.core.sink

import com.github.pflooky.datagen.core.exception.UnsupportedRealTimeDataSourceFormat
import com.github.pflooky.datagen.core.model.Constants.{ADVANCED_APPLICATION, BASIC_APPLICATION, BASIC_APPLICATION_SUPPORTED_CONNECTION_FORMATS, BATCH, DATA_CATERER_SITE_PRICING, DEFAULT_ROWS_PER_SECOND, DRIVER, FAILED, FINISHED, FORMAT, HTTP, JDBC, JMS, PARTITIONS, PER_COLUMN_INDEX_COL, POSTGRES_DRIVER, RATE, REAL_TIME, SAVE_MODE, STARTED}
import com.github.pflooky.datagen.core.model.Step
import com.github.pflooky.datagen.core.sink.http.HttpSinkProcessor
import com.github.pflooky.datagen.core.sink.jms.JmsSinkProcessor
import com.google.common.util.concurrent.RateLimiter
import org.apache.log4j.Logger
import org.apache.spark.sql.execution.streaming.sources.RateStreamProvider.ROWS_PER_SECOND
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class SinkFactory(
                   val connectionConfigs: Map[String, Map[String, String]],
                   val applicationType: String = "advanced"
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
    val count = if (enableCount) {
      df.count().toString
    } else {
      LOGGER.warn("Count is disabled. It will help with performance. Defaulting to -1")
      "-1"
    }
    LOGGER.info(s"Pushing data to sink, data-source-name=$dataSourceName, save-mode=$saveModeName, num-records=$count, status=$STARTED")
    saveData(df, dataSourceName, step, connectionConfig, saveMode, saveModeName, format, count)
  }

  private def saveData(df: DataFrame, dataSourceName: String, step: Step, connectionConfig: Map[String, String], saveMode: SaveMode, saveModeName: String, format: String, count: String): Unit = {
    val saveTiming = determineSaveTiming(dataSourceName, format)
    val trySaveData = Try(if (saveTiming.equalsIgnoreCase(BATCH)) {
      saveBatchData(df, saveMode, connectionConfig, step.options)
    } else if (applicationType.equalsIgnoreCase(ADVANCED_APPLICATION)) {
      saveRealTimeData(df, format, connectionConfig, step)
    } else if (applicationType.equalsIgnoreCase(BASIC_APPLICATION)) {
      LOGGER.warn(s"Please upgrade from the free plan to paid plan to enable generating real-time data. More details here: $DATA_CATERER_SITE_PRICING")
      return
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
    val format = connectionConfig(FORMAT)
    if (applicationType.equalsIgnoreCase(BASIC_APPLICATION) && (
      BASIC_APPLICATION_SUPPORTED_CONNECTION_FORMATS.contains(format) ||
        (format.equalsIgnoreCase(JDBC) && !connectionConfig(DRIVER).equalsIgnoreCase(POSTGRES_DRIVER)))) {
      LOGGER.warn(s"Please upgrade from the free plan to paid plan to enable generating data to all types of data source. " +
        s"Free tier only includes all file formats and Postgres. More details here: $DATA_CATERER_SITE_PRICING")
    }

    val partitionedDf = if (stepOptions.contains(PARTITIONS)) df.repartition(stepOptions(PARTITIONS).toInt) else df
    partitionedDf.write
      .format(format)
      .mode(saveMode)
      .options(connectionConfig)
      .options(stepOptions)
      .save()
  }

  private def saveRealTimeData(df: DataFrame, format: String, connectionConfig: Map[String, String], step: Step): Unit = {
    val rowsPerSecond = step.options.getOrElse(ROWS_PER_SECOND, DEFAULT_ROWS_PER_SECOND)
    LOGGER.info(s"Rows per second for generating data, rows-per-second=$rowsPerSecond")
    saveRealTimeGuava(df, format, connectionConfig, step, rowsPerSecond)
  }

  private def saveRealTimeGuava(df: DataFrame, format: String, connectionConfig: Map[String, String], step: Step, rowsPerSecond: String): Unit = {
    val rateLimiter = RateLimiter.create(rowsPerSecond.toInt)
    val sinkProcessor = getRealTimeSinkProcessor(format, connectionConfig, step)
    sinkProcessor.init()
    df.collect().foreach(row => {
      rateLimiter.acquire()
      sinkProcessor.pushRowToSink(row)
    })
  }

  @deprecated("Unstable for JMS connections")
  private def saveRealTimeSpark(df: DataFrame, format: String, connectionConfig: Map[String, String], step: Step, rowsPerSecond: String): Unit = {
    val dfWithIndex = df.selectExpr("*", s"monotonically_increasing_id() AS $PER_COLUMN_INDEX_COL")
    val rowCount = dfWithIndex.count().toInt
    val readStream = sparkSession.readStream
      .format(RATE).option(ROWS_PER_SECOND, rowsPerSecond)
      .load().limit(rowCount)

    val writeStream = readStream.writeStream
      .foreachBatch((batch: Dataset[Row], id: Long) => {
        LOGGER.info(s"batch num=$id, count=${batch.count()}")
        batch.join(dfWithIndex, batch("value") === dfWithIndex(PER_COLUMN_INDEX_COL))
          .drop(PER_COLUMN_INDEX_COL).repartition(3).rdd
          .foreachPartition(partition => {
            val part = partition.toList
            val sinkProcessor = getRealTimeSinkProcessor(format, connectionConfig, step)
            sinkProcessor.init()
            part.foreach(sinkProcessor.pushRowToSink)
            sinkProcessor.close
          })
      }).start()

    writeStream.awaitTermination(getTimeout(rowCount, rowsPerSecond.toInt))
  }

  private def getTimeout(totalRows: Int, rowsPerSecond: Int): Long = totalRows / rowsPerSecond * 1000

  private def getRealTimeSinkProcessor(format: String, connectionConfig: Map[String, String], step: Step): RealTimeSinkProcessor[_] = {
    format match {
      case HTTP => new HttpSinkProcessor(connectionConfig, step)
      case JMS => new JmsSinkProcessor(connectionConfig, step)
      case x => throw new UnsupportedRealTimeDataSourceFormat(x)
    }
  }
}
