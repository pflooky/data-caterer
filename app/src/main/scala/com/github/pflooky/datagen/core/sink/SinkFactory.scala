package com.github.pflooky.datagen.core.sink

import com.github.pflooky.datacaterer.api.model.Constants.{DRIVER, FORMAT, HTTP, JDBC, JMS, OMIT, PARTITIONS, PARTITION_BY, POSTGRES_DRIVER, RATE, ROWS_PER_SECOND, SAVE_MODE}
import com.github.pflooky.datacaterer.api.model.{FlagsConfig, MetadataConfig, Step}
import com.github.pflooky.datagen.core.model.Constants.{ADVANCED_APPLICATION, BASIC_APPLICATION, BASIC_APPLICATION_SUPPORTED_CONNECTION_FORMATS, BATCH, DATA_CATERER_SITE_PRICING, DEFAULT_ROWS_PER_SECOND, FAILED, FINISHED, PER_COLUMN_INDEX_COL, REAL_TIME, STARTED, TRIAL_APPLICATION}
import com.github.pflooky.datagen.core.model.SinkResult
import com.github.pflooky.datagen.core.sink.http.HttpSinkProcessor
import com.github.pflooky.datagen.core.util.KryoSerializationWrapper
import com.github.pflooky.datagen.core.util.MetadataUtil.getFieldMetadata
import com.google.common.util.concurrent.RateLimiter
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset, Encoders, Row, SaveMode, SparkSession}

import java.time.LocalDateTime
import java.util.concurrent.CompletableFuture
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class SinkFactory(
                   val connectionConfigs: Map[String, Map[String, String]],
                   val flagsConfig: FlagsConfig,
                   val metadataConfig: MetadataConfig,
                   val applicationType: String = "advanced"
                 )(implicit val sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private var HAS_LOGGED_COUNT_DISABLE_WARNING = false

  def pushToSink(df: DataFrame, dataSourceName: String, step: Step, flagsConfig: FlagsConfig, startTime: LocalDateTime): SinkResult = {
    val dfWithoutOmitFields = removeOmitFields(df)
    val connectionConfig = connectionConfigs.getOrElse(dataSourceName, Map(FORMAT -> step.`type`))
    val saveMode = connectionConfig.get(SAVE_MODE).map(_.toLowerCase.capitalize).map(SaveMode.valueOf).getOrElse(SaveMode.Append)
    val format = connectionConfig(FORMAT)
    val enrichedConnectionConfig = additionalConnectionConfig(format, connectionConfig)

    val count = if (flagsConfig.enableCount) {
      dfWithoutOmitFields.count().toString
    } else if (!HAS_LOGGED_COUNT_DISABLE_WARNING) {
      LOGGER.warn("Count is disabled. It will help with performance. Defaulting to -1")
      HAS_LOGGED_COUNT_DISABLE_WARNING = true
      "-1"
    } else "-1"
    LOGGER.info(s"Pushing data to sink, data-source-name=$dataSourceName, step-name=${step.name}, save-mode=${saveMode.name()}, num-records=$count, status=$STARTED")
    saveData(dfWithoutOmitFields, dataSourceName, step, enrichedConnectionConfig, saveMode, format, count, flagsConfig.enableFailOnError, startTime)
  }

  private def saveData(df: DataFrame, dataSourceName: String, step: Step, connectionConfig: Map[String, String],
                       saveMode: SaveMode, format: String, count: String, enableFailOnError: Boolean, startTime: LocalDateTime): SinkResult = {
    val saveTiming = determineSaveTiming(dataSourceName, format, step.name)
    val baseSinkResult = SinkResult(dataSourceName, format, saveMode.name())
    val sinkResult = if (saveTiming.equalsIgnoreCase(BATCH)) {
      saveBatchData(dataSourceName, df, saveMode, connectionConfig, step.options, count, startTime)
    } else if (applicationType.equalsIgnoreCase(ADVANCED_APPLICATION) || applicationType.equalsIgnoreCase(TRIAL_APPLICATION)) {
      saveRealTimeData(dataSourceName, df, format, connectionConfig, step, count, startTime)
    } else if (applicationType.equalsIgnoreCase(BASIC_APPLICATION)) {
      LOGGER.warn(s"Please upgrade from the free plan to paid plan to enable generating real-time data. More details here: $DATA_CATERER_SITE_PRICING")
      baseSinkResult
    } else baseSinkResult

    val finalSinkResult = (sinkResult.isSuccess, sinkResult.exception) match {
      case (false, Some(exception)) =>
        LOGGER.error(s"Failed to save data for sink, data-source-name=$dataSourceName, step-name=${step.name}, save-mode=${saveMode.name()}, " +
          s"num-records=$count, status=$FAILED, exception=${exception.getMessage.take(500)}")
        if (enableFailOnError) throw new RuntimeException(exception) else baseSinkResult
      case (true, None) =>
        LOGGER.info(s"Successfully saved data to sink, data-source-name=$dataSourceName, step-name=${step.name}, save-mode=${saveMode.name()}, " +
          s"num-records=$count, status=$FINISHED")
        sinkResult
      case (isSuccess, optException) =>
        LOGGER.warn(s"Unexpected sink result scenario, is-success=$isSuccess, exception-exists=${optException.isDefined}")
        sinkResult
    }
    df.unpersist()
    finalSinkResult
  }

  private def determineSaveTiming(dataSourceName: String, format: String, stepName: String): String = {
    format match {
      case HTTP | JMS =>
        LOGGER.debug(s"Given the step type is either HTTP or JMS, data will be generated in real-time mode. " +
          s"It will be based on requests per second defined at plan level, data-source-name=$dataSourceName, step-name=$stepName, format=$format")
        REAL_TIME
      case _ =>
        LOGGER.debug(s"Will generate data in batch mode for step, data-source-name=$dataSourceName, step-name=$stepName, format=$format")
        BATCH
    }
  }

  private def saveBatchData(dataSourceName: String, df: DataFrame, saveMode: SaveMode, connectionConfig: Map[String, String],
                            stepOptions: Map[String, String], count: String, startTime: LocalDateTime): SinkResult = {
    val format = connectionConfig(FORMAT)
    if (applicationType.equalsIgnoreCase(BASIC_APPLICATION) && (
      !BASIC_APPLICATION_SUPPORTED_CONNECTION_FORMATS.contains(format) ||
        (format.equalsIgnoreCase(JDBC) && !connectionConfig(DRIVER).equalsIgnoreCase(POSTGRES_DRIVER)))) {
      LOGGER.warn(s"Please upgrade from the free plan to paid plan to enable generating data to all types of data source. " +
        s"Free tier only includes all file formats and Postgres, data-source-name=$dataSourceName, format=$format. More details here: $DATA_CATERER_SITE_PRICING")
      return SinkResult(dataSourceName, format, saveMode.name())
    }

    val partitionedDf = partitionDf(df, stepOptions)
    val trySaveData = Try(partitionedDf
      .format(format)
      .mode(saveMode)
      .options(connectionConfig ++ stepOptions)
      .save())
    val optException = trySaveData match {
      case Failure(exception) => Some(exception)
      case Success(_) => None
    }
    mapToSinkResult(dataSourceName, df, saveMode, connectionConfig, stepOptions, count, format, trySaveData.isSuccess, startTime, optException)
  }

  private def partitionDf(df: DataFrame, stepOptions: Map[String, String]): DataFrameWriter[Row] = {
    val partitionDf = stepOptions.get(PARTITIONS)
      .map(partitionNum => df.repartition(partitionNum.toInt)).getOrElse(df)
    stepOptions.get(PARTITION_BY)
      .map(partitionCols => partitionDf.write.partitionBy(partitionCols.split(",").map(_.trim): _*))
      .getOrElse(partitionDf.write)
  }

  private def saveRealTimeData(dataSourceName: String, df: DataFrame, format: String, connectionConfig: Map[String, String],
                               step: Step, count: String, startTime: LocalDateTime): SinkResult = {
    val rowsPerSecond = step.options.getOrElse(ROWS_PER_SECOND, connectionConfig.getOrElse(ROWS_PER_SECOND, DEFAULT_ROWS_PER_SECOND))
    LOGGER.info(s"Rows per second for generating data, rows-per-second=$rowsPerSecond")
    saveRealTimeGuava(dataSourceName, df, format, connectionConfig, step, rowsPerSecond, count, startTime)
  }

  private def saveRealTimeGuava(dataSourceName: String, df: DataFrame, format: String, connectionConfig: Map[String, String],
                                step: Step, rowsPerSecond: String, count: String, startTime: LocalDateTime): SinkResult = {
    val saveResult = new ListBuffer[Try[Unit]]()
    val permitsPerSecond = Math.max(rowsPerSecond.toInt, 1)

    df.repartition(permitsPerSecond).foreachPartition((partition: Iterator[Row]) => {
      val rateLimiter = RateLimiter.create(1)
      val rows = partition.toList
      val sinkProcessor = SinkProcessor.getConnection(format, connectionConfig, step)

      rows.foreach(row => {
        rateLimiter.acquire()
        saveResult.append(Try(sinkProcessor.pushRowToSink(row)))
      })
      sinkProcessor.close
    })
    val CheckExceptionAndSuccess(optException, isSuccess) = checkExceptionAndSuccess(dataSourceName, format, step, count, saveResult)
    mapToSinkResult(dataSourceName, df, SaveMode.Append, connectionConfig, step.options, count, format, isSuccess, startTime, optException.headOption.flatten)
  }

  case class CheckExceptionAndSuccess(optException: ListBuffer[Option[Throwable]], isSuccess: Boolean)

  private def checkExceptionAndSuccess(dataSourceName: String, format: String, step: Step, count: String, saveResult: ListBuffer[Try[Unit]]): CheckExceptionAndSuccess = {
    val optException = saveResult.map {
      case Failure(exception) => Some(exception)
      case Success(_) => None
    }.filter(_.isDefined).distinct
    val optExceptionCount = optException.size

    val isSuccess = if (optExceptionCount > 1) {
      LOGGER.error(s"Multiple exceptions occurred when pushing to event sink, data-source-name=$dataSourceName, " +
        s"format=$format, step-name=${step.name}, exception-count=$optExceptionCount, record-count=$count")
      false
    } else if (optExceptionCount == 1) {
      false
    } else {
      true
    }
    CheckExceptionAndSuccess(optException, isSuccess)
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
            val sinkProcessor = SinkProcessor.getConnection(format, connectionConfig, step)
            part.foreach(sinkProcessor.pushRowToSink)
          })
      }).start()

    writeStream.awaitTermination(getTimeout(rowCount, rowsPerSecond.toInt))
  }

  private def getTimeout(totalRows: Int, rowsPerSecond: Int): Long = totalRows / rowsPerSecond * 1000

  private def additionalConnectionConfig(format: String, connectionConfig: Map[String, String]): Map[String, String] = {
    format match {
      case JDBC => if (connectionConfig(DRIVER).equalsIgnoreCase(POSTGRES_DRIVER) && !connectionConfig.contains("stringtype")) {
        connectionConfig ++ Map("stringtype" -> "unspecified")
      } else connectionConfig
      case _ => connectionConfig
    }
  }

  private def mapToSinkResult(dataSourceName: String, df: DataFrame, saveMode: SaveMode, connectionConfig: Map[String, String],
                              stepOptions: Map[String, String], count: String, format: String, isSuccess: Boolean, startTime: LocalDateTime,
                              optException: Option[Throwable]): SinkResult = {
    val cleansedOptions = cleanseOptions(connectionConfig, stepOptions)
    val sinkResult = SinkResult(dataSourceName, format, saveMode.name(), cleansedOptions, count.toLong, isSuccess, Array(), startTime, exception = optException)

    if (flagsConfig.enableSinkMetadata) {
      val sample = df.take(metadataConfig.numGeneratedSamples).map(_.json)
      val fields = getFieldMetadata(dataSourceName, df, connectionConfig, metadataConfig)
      sinkResult.copy(generatedMetadata = fields, sample = sample)
    } else {
      sinkResult
    }
  }

  private def cleanseOptions(connectionConfig: Map[String, String], stepOptions: Map[String, String]) = {
    (connectionConfig ++ stepOptions)
      .filter(o =>
        !(
          o._1.toLowerCase.contains("password") || o._2.toLowerCase.contains("password") ||
            o._1.toLowerCase.contains("token") ||
            o._1.toLowerCase.contains("secret") ||
            o._1.toLowerCase.contains("private")
          )
      )
  }

  private def removeOmitFields(df: DataFrame) = {
    val dfOmitFields = df.schema.fields
      .filter(field => field.metadata.contains(OMIT) && field.metadata.getString(OMIT).equalsIgnoreCase("true"))
      .map(_.name)
    val dfWithoutOmitFields = df.selectExpr(df.columns.filter(c => !dfOmitFields.contains(c)): _*)
    if (!dfWithoutOmitFields.storageLevel.useMemory) dfWithoutOmitFields.cache()
    dfWithoutOmitFields
  }
}
