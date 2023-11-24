package com.github.pflooky.datagen.core.sink

import com.github.pflooky.datacaterer.api.model.Constants.{DRIVER, FORMAT, JDBC, OMIT, PARTITIONS, PARTITION_BY, POSTGRES_DRIVER, RATE, ROWS_PER_SECOND, SAVE_MODE}
import com.github.pflooky.datacaterer.api.model.{FlagsConfig, MetadataConfig, Step}
import com.github.pflooky.datagen.core.model.Constants.{ADVANCED_APPLICATION, BASIC_APPLICATION, BASIC_APPLICATION_SUPPORTED_CONNECTION_FORMATS, BATCH, DATA_CATERER_SITE_PRICING, DEFAULT_ROWS_PER_SECOND, FAILED, FINISHED, PER_COLUMN_INDEX_COL, STARTED, TRIAL_APPLICATION}
import com.github.pflooky.datagen.core.model.SinkResult
import com.github.pflooky.datagen.core.util.ConfigUtil
import com.github.pflooky.datagen.core.util.GeneratorUtil.determineSaveTiming
import com.github.pflooky.datagen.core.util.MetadataUtil.getFieldMetadata
import com.google.common.util.concurrent.RateLimiter
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset, Encoder, Encoders, Row, SaveMode, SparkSession}

import java.time.LocalDateTime
import scala.util.{Failure, Success, Try}

class SinkFactory(
                   val flagsConfig: FlagsConfig,
                   val metadataConfig: MetadataConfig,
                   val applicationType: String = "advanced"
                 )(implicit val sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private var HAS_LOGGED_COUNT_DISABLE_WARNING = false

  def pushToSink(df: DataFrame, dataSourceName: String, step: Step, flagsConfig: FlagsConfig, startTime: LocalDateTime): SinkResult = {
    val dfWithoutOmitFields = removeOmitFields(df)
    val saveMode = step.options.get(SAVE_MODE).map(_.toLowerCase.capitalize).map(SaveMode.valueOf).getOrElse(SaveMode.Append)
    val format = step.options(FORMAT)
    val enrichedConnectionConfig = additionalConnectionConfig(format, step.options)

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
    //TODO might have use case where empty data can be tested, is it okay just to check for empty schema?
    val sinkResult = if (df.schema.isEmpty) {
      LOGGER.debug(s"Generated data schema is empty, not saving to data source, data-source-name=$dataSourceName, format=$format")
      baseSinkResult
    } else if (saveTiming.equalsIgnoreCase(BATCH)) {
      saveBatchData(dataSourceName, df, saveMode, connectionConfig, count, startTime)
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

  private def saveBatchData(dataSourceName: String, df: DataFrame, saveMode: SaveMode, connectionConfig: Map[String, String],
                            count: String, startTime: LocalDateTime): SinkResult = {
    val format = connectionConfig(FORMAT)
    if (applicationType.equalsIgnoreCase(BASIC_APPLICATION) && (
      !BASIC_APPLICATION_SUPPORTED_CONNECTION_FORMATS.contains(format) ||
        (format.equalsIgnoreCase(JDBC) && !connectionConfig(DRIVER).equalsIgnoreCase(POSTGRES_DRIVER)))) {
      LOGGER.warn(s"Please upgrade from the free plan to paid plan to enable generating data to all types of data source. " +
        s"Free tier only includes all file formats and Postgres, data-source-name=$dataSourceName, format=$format. More details here: $DATA_CATERER_SITE_PRICING")
      return SinkResult(dataSourceName, format, saveMode.name())
    }

    val partitionedDf = partitionDf(df, connectionConfig)
    val trySaveData = Try(partitionedDf
      .format(format)
      .mode(saveMode)
      .options(connectionConfig)
      .save())
    val optException = trySaveData match {
      case Failure(exception) => Some(exception)
      case Success(_) => None
    }
    mapToSinkResult(dataSourceName, df, saveMode, connectionConfig, count, format, trySaveData.isSuccess, startTime, optException)
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
    implicit val tryEncoder: Encoder[Try[Unit]] = Encoders.kryo[Try[Unit]]
    val permitsPerSecond = Math.max(rowsPerSecond.toInt, 1)

    //TODO should return back the response so it could be saved for potential validations (i.e. validate HTTP request and response)
    //if the real time data source is used as a data source for validations, save the df as parquet that can be used as the validation data source
    //folder path needs to be something that can be used in validation logic
    //or do we just always save the real time data as parquet and clean up (based on flag with default true) at the end of the job?
    val pushResults = df.repartition(permitsPerSecond).mapPartitions((partition: Iterator[Row]) => {
      val rateLimiter = RateLimiter.create(1)
      val rows = partition.toList
      val sinkProcessor = SinkProcessor.getConnection(format, connectionConfig, step)

      val pushResult = rows.map(row => {
        rateLimiter.acquire()
        Try(sinkProcessor.pushRowToSink(row))
      })
      sinkProcessor.close
      pushResult.toIterator
    })
    pushResults.cache()
    val CheckExceptionAndSuccess(optException, isSuccess) = checkExceptionAndSuccess(dataSourceName, format, step, count, pushResults)
    val someExp = if (optException.count() > 0) optException.head else None
    mapToSinkResult(dataSourceName, df, SaveMode.Append, connectionConfig, count, format, isSuccess, startTime, someExp)
  }

  case class CheckExceptionAndSuccess(optException: Dataset[Option[Throwable]], isSuccess: Boolean)

  private def checkExceptionAndSuccess(dataSourceName: String, format: String, step: Step, count: String, saveResult: Dataset[Try[Unit]]): CheckExceptionAndSuccess = {
    implicit val optionThrowableEncoder: Encoder[Option[Throwable]] = Encoders.kryo[Option[Throwable]]
    val optException = saveResult.map {
      case Failure(exception) => Some(exception)
      case Success(_) => None
    }.filter(_.isDefined).distinct
    val optExceptionCount = optException.count()

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
                              count: String, format: String, isSuccess: Boolean, startTime: LocalDateTime,
                              optException: Option[Throwable]): SinkResult = {
    val cleansedOptions = ConfigUtil.cleanseOptions(connectionConfig)
    val sinkResult = SinkResult(dataSourceName, format, saveMode.name(), cleansedOptions, count.toLong, isSuccess, Array(), startTime, exception = optException)

    if (flagsConfig.enableSinkMetadata) {
      val sample = df.take(metadataConfig.numGeneratedSamples).map(_.json)
      val fields = getFieldMetadata(dataSourceName, df, connectionConfig, metadataConfig)
      sinkResult.copy(generatedMetadata = fields, sample = sample)
    } else {
      sinkResult
    }
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
