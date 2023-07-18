package com.github.pflooky.datagen.core.generator.delete

import com.github.pflooky.datagen.core.model.Constants.{FORMAT, PATH}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class LocalFileDeleteRecordService extends DeleteRecordService {

  private val LOGGER = Logger.getLogger(getClass.getName)

  override def deleteRecords(dataSourceName: String, trackedRecords: DataFrame, options: Map[String, String])(implicit sparkSession: SparkSession): Unit = {
    val path = options(PATH)
    val format = options(FORMAT)
    LOGGER.warn(s"Deleting tracked generated records from local file, dataSourceName=$dataSourceName, path=$path")
    val df = sparkSession.read
      .format(format)
      .options(options)
      .load()
    df.join(trackedRecords, trackedRecords.columns, "left_anti")
      .write
      .mode(SaveMode.Overwrite)
      .format(format)
      .options(options)
      .save()
  }

}
