package com.github.pflooky.datagen.core.generator.track

import com.github.pflooky.datagen.core.model.Constants.{PARQUET, PATH}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DeleteRecordService extends Serializable {

  lazy val BATCH_SIZE = 1000

  def getTrackedRecords(dataSourcePath: String)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession.read.format(PARQUET)
      .option(PATH, dataSourcePath)
      .load()
  }

  def deleteRecords(dataSourceName: String, dataSourcePath: String, options: Map[String, String])(implicit sparkSession: SparkSession): Unit
}
