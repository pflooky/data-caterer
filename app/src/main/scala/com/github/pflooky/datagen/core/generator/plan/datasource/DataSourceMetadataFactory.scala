package com.github.pflooky.datagen.core.generator.plan.datasource

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.pflooky.datagen.core.model.Constants.{DRIVER, FORMAT, JDBC, POSTGRES_DRIVER}
import com.github.pflooky.datagen.core.model.{Plan, Task, TaskSummary}
import com.github.pflooky.datagen.core.util.SparkProvider
import org.apache.log4j.Logger
import org.joda.time.DateTime

import java.io.File

class DataSourceMetadataFactory extends SparkProvider {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val databaseMetadata = new DatabaseMetadata(sparkSession)
  private val OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
  OBJECT_MAPPER.registerModule(DefaultScalaModule)
  OBJECT_MAPPER.setSerializationInclusion(Include.NON_ABSENT)

  def extractAllDataSourceMetadata(): Unit = {
    if (enableGeneratePlanAndTasks) {
      LOGGER.info("Attempting to extract all data source metadata as defined in connection configurations in application.conf")
      val allConnectionMetadata = connectionConfigs.map(connectionConfig => {
        val metadata = connectionConfig._2(FORMAT) match {
          //        case CASSANDRA => Some(databaseMetadata.getCassandraTableMetadata(connectionConfig._2, List(), List()))
          case JDBC =>
            connectionConfig._2(DRIVER) match {
              case POSTGRES_DRIVER => Some((connectionConfig._1, databaseMetadata.getPostgresTableMetadata(connectionConfig, List(), List())))
              case _ => None
            }
          case _ => None
        }
        if (metadata.isEmpty) {
          LOGGER.warn(s"Metadata extraction not supported for connection type '${connectionConfig._2(FORMAT)}', connection-name=${connectionConfig._1}")
        }
        metadata
      })
      val allTaskMetadata = allConnectionMetadata.toList.filter(_.isDefined).map(_.get)
      writePlanAndTasksToFiles(allTaskMetadata, baseFolderPath)
    }
  }

  def writePlanAndTasksToFiles(tasks: List[(String, Task)], baseFolderPath: String): Unit = {
    val baseGeneratedFolder = new File(s"$baseFolderPath/generated")
    val planFolder = new File(s"${baseGeneratedFolder.getPath}/plan")
    val taskFolder = new File(s"${baseGeneratedFolder.getPath}/task")
    baseGeneratedFolder.mkdirs()
    planFolder.mkdirs()
    taskFolder.mkdirs()

    writePlanToFile(tasks, planFolder)
    writeTasksToFiles(tasks, taskFolder)
  }

  private def writePlanToFile(tasks: List[(String, Task)], planFolder: File): Unit = {
    val currentTime = new DateTime().toString("yyyy-MM-dd_hh:mm")
    val taskSummary = tasks.map(t => TaskSummary(t._2.name, t._1))
    //TODO: Need some way to understand potential relationships between fields of different data sources (i.e. correlations, word2vec) https://spark.apache.org/docs/latest/ml-features
    //would be included in the sink options below
    val plan = Plan(s"plan_$currentTime", "Generated plan", taskSummary, None)
    val generatedPlanFilePath = new File(s"$planFolder/plan_$currentTime.yaml")
    LOGGER.info(s"Writing plan to file, plan=${plan.name}, num-tasks=${plan.tasks.size}, file-path=$generatedPlanFilePath")
    generatedPlanFilePath.createNewFile()
    OBJECT_MAPPER.writeValue(generatedPlanFilePath, plan)
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
