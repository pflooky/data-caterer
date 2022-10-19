package com.github.pflooky.datagen.core.sink

import com.github.pflooky.datagen.core.model.{Task, TaskSummary}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class SinkFactory(
                   val tasks: List[(TaskSummary, Task)],
                   val connectionConfigs: Map[String, Map[String, String]]
                 )(implicit val sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def pushToSink(df: DataFrame, sinkName: String, stepOptions: Map[String, String]): Unit = {
    val connectionConfig = connectionConfigs(sinkName)
    val saveMode = connectionConfig.get("saveMode").map(SaveMode.valueOf).getOrElse(SaveMode.Append)
    LOGGER.info(s"Pushing data to sink=$sinkName, saveMode=${saveMode.name()}")

    df.write
      .format(connectionConfig("format"))
      .mode(saveMode)
      .options(connectionConfig)
      .options(stepOptions)
      .save()
  }

}
