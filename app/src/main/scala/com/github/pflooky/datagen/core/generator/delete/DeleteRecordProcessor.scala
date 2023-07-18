package com.github.pflooky.datagen.core.generator.delete

import com.github.pflooky.datagen.core.model.Constants.{CASSANDRA, CSV, DELTA, FORMAT, JDBC, JSON, ORC, PARQUET, PATH}
import com.github.pflooky.datagen.core.model.{ForeignKeyRelation, Plan, Step, Task, TaskSummary}
import com.github.pflooky.datagen.core.util.ForeignKeyUtil
import com.github.pflooky.datagen.core.util.MetadataUtil.getSubDataSourcePath
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import scala.reflect.io.Directory

class DeleteRecordProcessor(connectionConfigsByName: Map[String, Map[String, String]], recordTrackingFolderPath: String)(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def deleteGeneratedRecords(plan: Plan, stepsByName: Map[String, Step], summaryWithTask: List[(TaskSummary, Task)]): Unit = {
    if (plan.sinkOptions.isDefined && plan.sinkOptions.get.foreignKeys.nonEmpty) {
      val deleteOrder = ForeignKeyUtil.getDeleteOrder(plan.sinkOptions.get.foreignKeys)
      deleteOrder.foreach(foreignKeyName => {
        val foreignKeyRelation = ForeignKeyRelation.fromString(foreignKeyName)
        val connectionConfig = connectionConfigsByName(foreignKeyRelation.dataSource)
        val format = connectionConfig(FORMAT)
        val step = stepsByName(foreignKeyRelation.step)
        deleteRecords(foreignKeyRelation.dataSource, format, step.options, connectionConfig)
      })
    } else {
      summaryWithTask.foreach(task => {
        task._2.steps.foreach(step => {
          val connectionConfig = connectionConfigsByName(task._1.dataSourceName)
          val format = connectionConfig(FORMAT)
          deleteRecords(task._1.dataSourceName, format, step.options, connectionConfig)
        })
      })
    }
  }

  def deleteRecords(dataSourceName: String, format: String, options: Map[String, String], connectionConfig: Map[String, String]): Unit = {
    val subDataSourcePath = getSubDataSourcePath(dataSourceName, format, options, recordTrackingFolderPath)
    val deleteRecordService = format.toLowerCase match {
      case JDBC => new JdbcDeleteRecordService
      case CASSANDRA => new CassandraDeleteRecordService
      case PARQUET | JSON | CSV | ORC | DELTA => new LocalFileDeleteRecordService
    }
    LOGGER.warn(s"Delete generated records is enabled. If 'enableRecordTracking' has been enabled, all generated records for this data source will be deleted, " +
      s"data-source-name=$dataSourceName, format=$format, details=$options")
    val trackedRecords = getTrackedRecords(subDataSourcePath)
    deleteRecordService.deleteRecords(dataSourceName, trackedRecords, options ++ connectionConfig)
    deleteTrackedRecordsFile(subDataSourcePath)
  }

  def getTrackedRecords(dataSourcePath: String): DataFrame = {
    sparkSession.read.format(PARQUET)
      .option(PATH, dataSourcePath)
      .load()
  }

  def deleteTrackedRecordsFile(dataSourcePath: String): Unit = {
    new Directory(new File(dataSourcePath)).deleteRecursively()
  }

}
