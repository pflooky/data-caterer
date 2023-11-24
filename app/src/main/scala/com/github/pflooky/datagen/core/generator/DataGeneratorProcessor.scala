package com.github.pflooky.datagen.core.generator

import com.github.pflooky.datacaterer.api.model.{DataCatererConfiguration, Plan, Task, TaskSummary, ValidationConfiguration}
import com.github.pflooky.datagen.core.config.ConfigParser
import com.github.pflooky.datagen.core.generator.delete.DeleteRecordProcessor
import com.github.pflooky.datagen.core.generator.result.DataGenerationResultWriter
import com.github.pflooky.datagen.core.listener.SparkRecordListener
import com.github.pflooky.datagen.core.model.Constants.{ADVANCED_APPLICATION, BASIC_APPLICATION, DATA_CATERER_SITE_PRICING, TRIAL_APPLICATION}
import com.github.pflooky.datagen.core.model.PlanImplicits.TaskOps
import com.github.pflooky.datagen.core.parser.PlanParser
import com.github.pflooky.datagen.core.validator.ValidationProcessor
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

class DataGeneratorProcessor(dataCatererConfiguration: DataCatererConfiguration)(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val connectionConfigsByName = dataCatererConfiguration.connectionConfigByName
  private val foldersConfig = dataCatererConfiguration.foldersConfig
  private val metadataConfig = dataCatererConfiguration.metadataConfig
  private val flagsConfig = dataCatererConfiguration.flagsConfig
  private val generationConfig = dataCatererConfiguration.generationConfig
  private lazy val deleteRecordProcessor = new DeleteRecordProcessor(connectionConfigsByName, foldersConfig.recordTrackingFolderPath)
  private lazy val dataGenerationResultWriter = new DataGenerationResultWriter(metadataConfig, foldersConfig, flagsConfig)
  private lazy val batchDataProcessor = new BatchDataProcessor(connectionConfigsByName, foldersConfig, metadataConfig, flagsConfig, generationConfig, applicationType)
  private lazy val sparkRecordListener = new SparkRecordListener(flagsConfig.enableCount)
  val applicationType: String = ConfigParser.applicationType
  sparkSession.sparkContext.addSparkListener(sparkRecordListener)

  def generateData(): Unit = {
    val plan = PlanParser.parsePlan(foldersConfig.planFilePath)
    val enabledPlannedTasks = plan.tasks.filter(_.enabled)
    val enabledTaskMap = enabledPlannedTasks.map(t => (t.name, t)).toMap
    val tasks = PlanParser.parseTasks(foldersConfig.taskFolderPath)
    val enabledTasks = tasks.filter(t => enabledTaskMap.contains(t.name)).toList

    generateData(plan.copy(tasks = enabledPlannedTasks), enabledTasks, None)
  }

  def generateData(plan: Plan, tasks: List[Task], optValidations: Option[List[ValidationConfiguration]]): Unit = {
    val tasksByName = tasks.map(t => (t.name, t)).toMap
    val summaryWithTask = plan.tasks.map(t => (t, tasksByName(t.name)))

    generateDataWithResult(plan, summaryWithTask, optValidations)
    (flagsConfig.enableDeleteGeneratedRecords, applicationType) match {
      case (true, ADVANCED_APPLICATION | TRIAL_APPLICATION) =>
        val stepsByName = tasks.flatMap(_.steps).filter(_.enabled).map(s => (s.name, s)).toMap
        deleteRecordProcessor.deleteGeneratedRecords(plan, stepsByName, summaryWithTask)
      case (true, BASIC_APPLICATION) =>
        LOGGER.warn(s"Please upgrade from the free plan to paid plan to enable generated records to be deleted. More details here: $DATA_CATERER_SITE_PRICING")
      case _ => //do nothing
    }
  }

  private def generateDataWithResult(plan: Plan, summaryWithTask: List[(TaskSummary, Task)], optValidations: Option[List[ValidationConfiguration]]): Unit = {
    if (flagsConfig.enableDeleteGeneratedRecords) {
      LOGGER.warn("Both enableGenerateData and enableDeleteGeneratedData are true. Please only enable one at a time. Will continue with generating data")
    }
    if (LOGGER.isDebugEnabled) {
      LOGGER.debug(s"Following tasks are enabled and will be executed: num-tasks=${summaryWithTask.size}, tasks=($summaryWithTask)")
      summaryWithTask.foreach(t => LOGGER.debug(s"Enabled task details: ${t._2.toTaskDetailString}"))
    }
    val stepNames = summaryWithTask.map(t => s"task=${t._2.name}, num-steps=${t._2.steps.size}, steps=${t._2.steps.map(_.name).mkString(",")}").mkString("||")

    if (summaryWithTask.isEmpty) {
      LOGGER.warn("No tasks found or no tasks enabled. No data will be generated")
    } else {
      val generationResult = if (flagsConfig.enableGenerateData) {
        LOGGER.info(s"Following tasks are enabled and will be executed: num-tasks=${summaryWithTask.size}, tasks=$stepNames")
        batchDataProcessor.splitAndProcess(plan, summaryWithTask, optValidations)
      } else List()

      val validationResults = if (flagsConfig.enableValidation) {
        new ValidationProcessor(connectionConfigsByName, optValidations, dataCatererConfiguration.validationConfig, foldersConfig)
          .executeValidations
      } else List()

      if (flagsConfig.enableSaveReports) {
        dataGenerationResultWriter.writeResult(plan, generationResult, validationResults, sparkRecordListener)
      }
    }
  }

}
