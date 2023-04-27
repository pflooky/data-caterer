package com.github.pflooky.datagen.core.generator.plan.datasource

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.pflooky.datagen.core.generator.plan.datasource.database.{CassandraMetadata, DatabaseMetadata, DatabaseMetadataProcessor, PostgresMetadata}
import com.github.pflooky.datagen.core.model.Constants.{CASSANDRA, DRIVER, FORMAT, JDBC, POSTGRES_DRIVER}
import com.github.pflooky.datagen.core.model.{Plan, SinkOptions, Task, TaskSummary}
import com.github.pflooky.datagen.core.util.{ForeignKeyUtil, MetadataUtil, SparkProvider}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import org.joda.time.DateTime

import java.io.File

class DataSourceMetadataFactory extends SparkProvider {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val databaseMetadataProcessor = new DatabaseMetadataProcessor(sparkSession)
  private val OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
  OBJECT_MAPPER.registerModule(DefaultScalaModule)
  OBJECT_MAPPER.setSerializationInclusion(Include.NON_ABSENT)

  def extractAllDataSourceMetadata(): Option[(Plan, List[Task])] = {
    if (enableGeneratePlanAndTasks) {
      LOGGER.info("Attempting to extract all data source metadata as defined in connection configurations in application.conf")
      val connectionMetadata = connectionConfigsByName.map(connectionConfig => {
        val connection = connectionConfig._2(FORMAT) match {
          case CASSANDRA => Some(CassandraMetadata(connectionConfig._1, connectionConfig._2))
          case JDBC =>
            connectionConfig._2(DRIVER) match {
              case POSTGRES_DRIVER => Some(PostgresMetadata(connectionConfig._1, connectionConfig._2))
              case _ => None
            }
          case _ => None
        }
        if (connection.isEmpty) {
          LOGGER.warn(s"Metadata extraction not supported for connection type '${connectionConfig._2(FORMAT)}', connection-name=${connectionConfig._1}")
        }
        connection
      }).toList

      val metadataPerConnection = connectionMetadata.filter(_.isDefined).map(_.get).map(x => {
        (x, x.getForeignKeys, getMetadataForDataSource(x))
      })
      val generatedTasksFromMetadata = metadataPerConnection.map(m => (m._1.name, Task.fromMetadata(m._1.name, m._1.format, m._3)))
      //given all the foreign key relations in each data source, detect if there are any links between data sources, then pass that into plan
      val allForeignKeys = ForeignKeyUtil.getAllForeignKeyRelationships(metadataPerConnection.map(_._2))
      Some(writePlanAndTasksToFiles(generatedTasksFromMetadata, allForeignKeys, baseFolderPath))
    } else None
  }

  def getMetadataForDataSource(dataSourceMetadata: DataSourceMetadata): List[DataSourceDetail] = {
    LOGGER.info(s"Extracting out metadata from data source, name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}")
    val allDataSourceReadOptions = dataSourceMetadata match {
      case databaseMetadata: DatabaseMetadata => databaseMetadataProcessor.getAllDatabaseTables(databaseMetadata)
      case _ =>
        //TODO: given file system based data source, traverse the folders to find all possible data paths (i.e. could be parquet files, csv, json, delta etc.)
        Array[Map[String, String]]()
    }

    val numSubDataSources = allDataSourceReadOptions.length
    if (numSubDataSources == 0) {
      LOGGER.warn(s"Unable to find any sub data sources, name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}")
    } else {
      LOGGER.info(s"Found sub data sources, name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}, num-sub-data-sources=$numSubDataSources")
    }
    val additionalColumnMetadata = dataSourceMetadata.getAdditionalColumnMetadata

    allDataSourceReadOptions.map(dataSourceReadOptions => {
      val data = sparkSession.read
        .format(dataSourceMetadata.format)
        .options(dataSourceMetadata.connectionConfig ++ dataSourceReadOptions)
        .load()
      val fieldsWithDataProfilingMetadata = MetadataUtil.getFieldDataProfilingMetadata(sparkSession, data, dataSourceReadOptions, dataSourceMetadata)
      val structFields = MetadataUtil.mapToStructFields(sparkSession, data, dataSourceReadOptions, fieldsWithDataProfilingMetadata, additionalColumnMetadata)
      DataSourceDetail(dataSourceMetadata, dataSourceReadOptions, StructType(structFields))
    }).toList
  }

  def writePlanAndTasksToFiles(tasks: List[(String, Task)], foreignKeys: Map[String, List[String]], baseFolderPath: String): (Plan, List[Task]) = {
    val baseGeneratedFolder = new File(s"$baseFolderPath/generated")
    val planFolder = new File(s"${baseGeneratedFolder.getPath}/plan")
    val taskFolder = new File(s"${baseGeneratedFolder.getPath}/task")
    baseGeneratedFolder.mkdirs()
    planFolder.mkdirs()
    taskFolder.mkdirs()

    val plan = writePlanToFile(tasks, foreignKeys, planFolder)
    writeTasksToFiles(tasks, taskFolder)
    (plan, tasks.map(_._2))
  }

  private def writePlanToFile(tasks: List[(String, Task)], foreignKeys: Map[String, List[String]], planFolder: File): Plan = {
    val currentTime = new DateTime().toString("yyyy-MM-dd_HH:mm")
    val taskSummary = tasks.map(t => TaskSummary(t._2.name, t._1))
    val plan = Plan(s"plan_$currentTime", "Generated plan", taskSummary, Some(SinkOptions(None, None, foreignKeys)))
    val generatedPlanFilePath = new File(s"$planFolder/plan_$currentTime.yaml")
    LOGGER.info(s"Writing plan to file, plan=${plan.name}, num-tasks=${plan.tasks.size}, file-path=$generatedPlanFilePath")
    generatedPlanFilePath.createNewFile()
    OBJECT_MAPPER.writeValue(generatedPlanFilePath, plan)
    plan
  }

  private def writeTasksToFiles(tasks: List[(String, Task)], taskFolder: File): Unit = {
    tasks.map(_._2).foreach(task => {
      val taskFileName = s"${task.name}_task.yaml"
      val taskFilePath = new File(s"$taskFolder/$taskFileName")
      LOGGER.info(s"Writing task to file, task=${task.name}, num-steps=${task.steps.size}, file-path=$taskFilePath")
      if (taskFilePath.exists()) taskFilePath.delete()
      taskFilePath.createNewFile()
      OBJECT_MAPPER.writeValue(taskFilePath, task)
    })
  }
}

case class DataSourceDetail(dataSourceMetadata: DataSourceMetadata, sparkOptions: Map[String, String], structType: StructType)
