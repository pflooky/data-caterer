package com.github.pflooky.datagen.core.generator.delete

import com.github.pflooky.datacaterer.api.model.Constants.{CASSANDRA, CSV, DELTA, FORMAT, JDBC, JSON, ORC, PARQUET, PATH}
import com.github.pflooky.datacaterer.api.model.{Plan, Step, Task, TaskSummary}
import com.github.pflooky.datagen.core.model.ForeignKeyRelationHelper
import com.github.pflooky.datagen.core.model.PlanImplicits.SinkOptionsOps
import com.github.pflooky.datagen.core.util.ForeignKeyUtil
import com.github.pflooky.datagen.core.util.MetadataUtil.getSubDataSourcePath
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import scala.reflect.io.Directory
import scala.util.{Failure, Success, Try}

class DeleteRecordProcessor(connectionConfigsByName: Map[String, Map[String, String]], recordTrackingFolderPath: String)(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def deleteGeneratedRecords(plan: Plan, stepsByName: Map[String, Step], summaryWithTask: List[(TaskSummary, Task)]): Unit = {
    if (plan.sinkOptions.isDefined && plan.sinkOptions.get.foreignKeys.nonEmpty) {
      val deleteOrder = ForeignKeyUtil.getDeleteOrder(plan.sinkOptions.get.foreignKeysWithoutColumnNames)
      deleteOrder.foreach(foreignKeyName => {
        val foreignKeyRelation = ForeignKeyRelationHelper.fromString(foreignKeyName)
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
    val tryTrackedRecords = Try(getTrackedRecords(subDataSourcePath))
    tryTrackedRecords match {
      case Failure(exception) =>
        LOGGER.error(s"Failed to get tracked records for data source, will continue to try delete other data sources, data-source-name=$dataSourceName, format=$format, details=$options, exception=${exception.getMessage}")
      case Success(trackedRecords) =>
        deleteRecordService.deleteRecords(dataSourceName, trackedRecords, options ++ connectionConfig)
        deleteTrackedRecordsFile(subDataSourcePath)
    }
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
