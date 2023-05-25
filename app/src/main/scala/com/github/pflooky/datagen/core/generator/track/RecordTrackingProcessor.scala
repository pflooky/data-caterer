package com.github.pflooky.datagen.core.generator.track

import com.github.pflooky.datagen.core.model.Constants.{CASSANDRA, CASSANDRA_KEYSPACE, CASSANDRA_TABLE, CSV, DELTA, HTTP, HTTP_METHOD, IS_PRIMARY_KEY, JDBC, JDBC_TABLE, JMS, JMS_DESTINATION_NAME, JSON, ORC, PARQUET, PATH, PRIMARY_KEY_POSITION}
import com.github.pflooky.datagen.core.model.Step
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File
import scala.reflect.io.Directory

class RecordTrackingProcessor(recordTrackingFolderPath: String)(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def trackRecords(df: DataFrame, dataSourceName: String, format: String, step: Step): Unit = {
    LOGGER.info(s"Generated record tracking is enabled, data-source-name=$dataSourceName")
    val primaryKeys = step.getPrimaryKeys
    val columnsToTrack = if (primaryKeys.isEmpty) df.columns.toList else primaryKeys
    val subDataSourcePath = getSubDataSourcePath(dataSourceName, format, step.options)
    df.selectExpr(columnsToTrack: _*)
      .write.format(PARQUET)
      .mode(SaveMode.Append)
      .option(PATH, subDataSourcePath)
      .save()
  }

  def getTrackedRecords(dataSourcePath: String)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession.read.format(PARQUET)
      .option(PATH, dataSourcePath)
      .load()
  }

  def deleteRecords(dataSourceName: String, format: String, options: Map[String, String], connectionConfig: Map[String, String]): Unit = {
    val subDataSourcePath = getSubDataSourcePath(dataSourceName, format, options)
    val deleteRecordService = format.toLowerCase match {
      case JDBC => new JdbcDeleteRecordService
      case CASSANDRA => new CassandraDeleteRecordService
    }
    LOGGER.warn(s"Delete generated records is enabled. If 'enableRecordTracking' has been enabled, all generated records for this data source will be deleted, " +
      s"data-source-name=$dataSourceName, format=$format, details=$options")
    val trackedRecords = getTrackedRecords(subDataSourcePath)
    deleteRecordService.deleteRecords(dataSourceName, trackedRecords, options ++ connectionConfig)
    deleteTrackedRecordsFile(subDataSourcePath)
  }

  def deleteTrackedRecordsFile(dataSourcePath: String): Unit = {
    new Directory(new File(dataSourcePath)).deleteRecursively()
  }

  private def getSubDataSourcePath(dataSourceName: String, format: String, options: Map[String, String]): String = {
    val lowerFormat = format.toLowerCase
    val subPath = lowerFormat match {
      case JDBC =>
        val spt = options(JDBC_TABLE).split("\\.")
        val (schema, table) = (spt.head, spt.last)
        s"$schema/$table"
      case CASSANDRA =>
        s"${options(CASSANDRA_KEYSPACE)}/${options(CASSANDRA_TABLE)}"
      case PARQUET | CSV | JSON | DELTA | ORC =>
        options(PATH).replaceAll("s3(a|n?)://|wasb(s?)://|gs://|file://|hdfs://[a-zA-Z0-9]+:[0-9]+", "")
      case JMS =>
        options(JMS_DESTINATION_NAME)
      case HTTP =>
        //TODO do we support delete via HTTP?
        options(HTTP_METHOD)
      case _ =>
        throw new RuntimeException(s"Unsupported data format for record tracking, format=$lowerFormat")
    }
    s"$recordTrackingFolderPath/$lowerFormat/$dataSourceName/$subPath"
  }
}
