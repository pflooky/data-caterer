package com.github.pflooky.datagen.core.sink

import com.github.pflooky.datagen.core.model.Constants.{FAILED, FINISHED, FORMAT, SAVE_MODE, STARTED}
import com.github.pflooky.datagen.core.model.{Task, TaskSummary}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

class SinkFactory(
                   val tasks: List[(TaskSummary, Task)],
                   val connectionConfigs: Map[String, Map[String, String]]
                 )(implicit val sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def pushToSink(df: DataFrame, dataSourceName: String, stepOptions: Map[String, String], enableCount: Boolean): Unit = {
    if (!connectionConfigs.contains(dataSourceName)) {
      throw new RuntimeException(s"Cannot find sink connection details in application.conf for data source: $dataSourceName")
    }
    val connectionConfig = connectionConfigs(dataSourceName)
    val saveMode = connectionConfig.get(SAVE_MODE).map(_.toLowerCase.capitalize).map(SaveMode.valueOf).getOrElse(SaveMode.Append)
    val saveModeName = saveMode.name()
    df.cache()
    val count = if (enableCount) {
      df.count().toString
    } else {
      LOGGER.warn("Count is disabled. It will help with performance. Defaulting to -1")
      "-1"
    }
    LOGGER.info(s"Pushing data to sink, data-source-name=$dataSourceName, save-mode=$saveModeName, num-records=$count, status=$STARTED")

    val trySaveData = Try(df.write
      .format(connectionConfig(FORMAT))
      .mode(saveMode)
      .options(connectionConfig)
      .options(stepOptions)
      .save())
    trySaveData match {
      case Failure(exception) =>
        throw new RuntimeException(s"Failed to save data for sink, data-source-name=$dataSourceName, save-mode=$saveModeName, num-records=$count, status=$FAILED", exception)
      case Success(_) =>
        LOGGER.info(s"Successfully saved data to sink, data-source-name=$dataSourceName, save-mode=$saveModeName, num-records=$count, status=$FINISHED")
    }
  }

}
