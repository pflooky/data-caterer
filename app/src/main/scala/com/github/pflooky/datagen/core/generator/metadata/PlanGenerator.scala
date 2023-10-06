package com.github.pflooky.datagen.core.generator.metadata

import com.github.pflooky.datacaterer.api.model.{FoldersConfig, Plan, SinkOptions, Task, TaskSummary, ValidationConfiguration}
import com.github.pflooky.datagen.core.util.FileUtil.writeStringToFile
import com.github.pflooky.datagen.core.util.ObjectMapperUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

object PlanGenerator {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OBJECT_MAPPER = ObjectMapperUtil.yamlObjectMapper

  def writeToFiles(
                    tasks: List[(String, Task)],
                    foreignKeys: List[(String, List[String])],
                    validationConfig: ValidationConfiguration,
                    foldersConfig: FoldersConfig
                  )(implicit sparkSession: SparkSession): (Plan, List[Task], ValidationConfiguration) = {
    val baseFolderPath = foldersConfig.generatedPlanAndTaskFolderPath
    val fileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    fileSystem.setWriteChecksum(false)
    val plan = writePlanToFile(tasks, foreignKeys, s"$baseFolderPath/plan", fileSystem)
    writeTasksToFiles(tasks, s"$baseFolderPath/task", fileSystem)
    writeValidationsToFiles(validationConfig, s"$baseFolderPath/validation", fileSystem)
    fileSystem.close()
    (plan, tasks.map(_._2), validationConfig)
  }

  private def writePlanToFile(tasks: List[(String, Task)], foreignKeys: List[(String, List[String])], planFolder: String, fileSystem: FileSystem)
                             (implicit sparkSession: SparkSession): Plan = {
    val currentTime = new DateTime().toString(ISODateTimeFormat.basicDateTimeNoMillis())
    val taskSummary = tasks.map(t => TaskSummary(t._2.name, t._1))
    val plan = Plan(s"plan_$currentTime", "Generated plan", taskSummary, Some(SinkOptions(None, None, foreignKeys)))
    val planFilePath = s"$planFolder/plan_$currentTime.yaml"
    LOGGER.info(s"Writing plan to file, plan=${plan.name}, num-tasks=${plan.tasks.size}, file-path=$planFilePath")
    val fileContent = OBJECT_MAPPER.writeValueAsString(plan)
    writeStringToFile(fileSystem, planFilePath, fileContent)
    plan
  }

  private def writeTasksToFiles(tasks: List[(String, Task)], taskFolder: String, fileSystem: FileSystem)(implicit sparkSession: SparkSession): Unit = {
    tasks.map(_._2).foreach(task => {
      val taskFilePath = s"$taskFolder/${task.name}_task.yaml"
      LOGGER.info(s"Writing task to file, task=${task.name}, num-steps=${task.steps.size}, file-path=$taskFilePath")
      val fileContent = OBJECT_MAPPER.writeValueAsString(task)
      writeStringToFile(fileSystem, taskFilePath, fileContent)
    })
  }

  private def writeValidationsToFiles(
                                       validationConfiguration: ValidationConfiguration,
                                       folder: String,
                                       fileSystem: FileSystem
                                     )(implicit sparkSession: SparkSession): Unit = {
    val taskFilePath = s"$folder/validations.yaml"
    val numValidations = validationConfiguration.dataSources.flatMap(_._2.head.validations).size
    LOGGER.info(s"Writing validations to file, num-validations=$numValidations, file-path=$taskFilePath")
    val fileContent = OBJECT_MAPPER.writeValueAsString(validationConfiguration)
    writeStringToFile(fileSystem, taskFilePath, fileContent)
  }
}
