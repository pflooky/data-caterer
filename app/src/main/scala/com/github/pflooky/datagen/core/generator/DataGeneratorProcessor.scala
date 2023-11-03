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
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.io.Serializable
import java.util.{Locale, Random}
import scala.util.{Failure, Success, Try}

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
    val faker = getDataFaker(plan)

    (flagsConfig.enableGenerateData, flagsConfig.enableDeleteGeneratedRecords, applicationType) match {
      case (true, _, _) =>
        generateData(plan, summaryWithTask, optValidations, faker)
      case (_, true, ADVANCED_APPLICATION | TRIAL_APPLICATION) =>
        val stepsByName = tasks.flatMap(_.steps).filter(_.enabled).map(s => (s.name, s)).toMap
        deleteRecordProcessor.deleteGeneratedRecords(plan, stepsByName, summaryWithTask)
      case (_, true, BASIC_APPLICATION) =>
        LOGGER.warn(s"Please upgrade from the free plan to paid plan to enable generated records to be deleted. More details here: $DATA_CATERER_SITE_PRICING")
      case _ =>
        LOGGER.warn("Data generation is disabled")
    }
  }

  private def generateData(plan: Plan, summaryWithTask: List[(TaskSummary, Task)], optValidations: Option[List[ValidationConfiguration]], faker: Faker with Serializable): Unit = {
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
      LOGGER.info(s"Following tasks are enabled and will be executed: num-tasks=${summaryWithTask.size}, tasks=$stepNames")
      val generationResult = batchDataProcessor.splitAndProcess(plan, summaryWithTask, faker)
      val validationResults = new ValidationProcessor(flagsConfig.enableValidation, connectionConfigsByName, optValidations, foldersConfig.validationFolderPath)
        .executeValidations

      if (flagsConfig.enableSaveReports) {
        dataGenerationResultWriter.writeResult(plan, generationResult, validationResults, sparkRecordListener)
      }
    }
  }

  private def getDataFaker(plan: Plan): Faker with Serializable = {
    val optSeed = plan.sinkOptions.flatMap(_.seed)
    val optLocale = plan.sinkOptions.flatMap(_.locale)
    val trySeed = Try(optSeed.map(_.toInt).get)

    (optSeed, trySeed, optLocale) match {
      case (None, _, Some(locale)) =>
        LOGGER.info(s"Locale defined at plan level. All data will be generated with the set locale, locale=$locale")
        new Faker(Locale.forLanguageTag(locale)) with Serializable
      case (Some(_), Success(seed), Some(locale)) =>
        LOGGER.info(s"Seed and locale defined at plan level. All data will be generated with the set seed and locale, seed-value=$seed, locale=$locale")
        new Faker(Locale.forLanguageTag(locale), new Random(seed)) with Serializable
      case (Some(_), Failure(exception), _) =>
        throw new RuntimeException("Failed to get seed value from plan sink options", exception)
      case _ => new Faker() with Serializable
    }
  }

}
