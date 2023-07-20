package com.github.pflooky.datagen.core.generator

import com.github.pflooky.datagen.core.exception.InvalidCountGeneratorConfigurationException
import com.github.pflooky.datagen.core.generator.delete.DeleteRecordProcessor
import com.github.pflooky.datagen.core.generator.track.RecordTrackingProcessor
import com.github.pflooky.datagen.core.model.Constants.{ADVANCED_APPLICATION, BASIC_APPLICATION, DATA_CATERER_SITE_PRICING, FORMAT, RECORD_COUNT_GENERATOR_COL}
import com.github.pflooky.datagen.core.model.{Generator, Plan, Step, Task, TaskSummary}
import com.github.pflooky.datagen.core.parser.PlanParser
import com.github.pflooky.datagen.core.sink.SinkFactory
import com.github.pflooky.datagen.core.util.GeneratorUtil.{getDataGenerator, getRecordCount}
import com.github.pflooky.datagen.core.util.{ForeignKeyUtil, ObjectMapperUtil, SparkProvider}
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{LongType, Metadata, StructField}

import java.io.Serializable
import java.util.{Locale, Random}
import scala.util.{Failure, Success, Try}

class DataGeneratorProcessor extends SparkProvider {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private lazy val recordTrackingFactory = new RecordTrackingProcessor(foldersConfig.recordTrackingFolderPath)
  private lazy val deleteRecordProcessor = new DeleteRecordProcessor(connectionConfigsByName, foldersConfig.recordTrackingFolderPath)

  def generateData(): Unit = {
    val plan = PlanParser.parsePlan(foldersConfig.planFilePath)
    val enabledPlannedTasks = plan.tasks.filter(_.enabled)
    val enabledTaskMap = enabledPlannedTasks.map(t => (t.name, t)).toMap
    val tasks = PlanParser.parseTasks(foldersConfig.taskFolderPath)

    generateData(plan.copy(tasks = enabledPlannedTasks), tasks.filter(t => enabledTaskMap.contains(t.name)).toList)
  }

  def generateData(plan: Plan, tasks: List[Task]): Unit = {
    val tasksByName = tasks.map(t => (t.name, t)).toMap
    val stepsByName = tasks.flatMap(_.steps).map(s => (s.name, s)).toMap
    val summaryWithTask = plan.tasks.map(t => (t, tasksByName(t.name)))

    if (flagsConfig.enableGenerateData && flagsConfig.enableDeleteGeneratedRecords) {
      LOGGER.warn("Both enableGenerateData and enableDeleteGeneratedData are true. Please only enable one at a time. Will continue with generating data")
    }

    (flagsConfig.enableGenerateData, flagsConfig.enableDeleteGeneratedRecords, applicationType) match {
      case (true, _, _) =>
        LOGGER.info(s"Following tasks are enabled and will be executed: num-tasks=${summaryWithTask.size}, tasks=($summaryWithTask)")
        summaryWithTask.foreach(t => LOGGER.info(s"Enabled task details: ${t._2.toTaskDetailString}"))
        //TODO batch up 2000 records total across all steps and progressively push to sinks
        //would need to keep track of the number of records produced per sink and stop when requested count is reached
        val sinkDf = getAllStepDf(plan, summaryWithTask)
        pushDataToSinks(summaryWithTask, sinkDf)
      case (_, true, ADVANCED_APPLICATION) =>
        deleteRecordProcessor.deleteGeneratedRecords(plan, stepsByName, summaryWithTask)
      case (_, true, BASIC_APPLICATION) =>
        LOGGER.warn(s"Please upgrade from the free plan to paid plan to enable generated records to be deleted. More details here: $DATA_CATERER_SITE_PRICING")
      case _ =>
        LOGGER.warn("Data generation is disabled")
    }
  }

  private def getAllStepDf(plan: Plan, executableTasks: List[(TaskSummary, Task)]): Map[String, DataFrame] = {
    val faker = getDataFaker(plan)
    val dataGeneratorFactory = new DataGeneratorFactory(faker)
    val generatedDataForeachTask = executableTasks.flatMap(task =>
      task._2.steps.map(s => (getDataSourceName(task._1, s), dataGeneratorFactory.generateDataForStep(s, task._1.dataSourceName)))
    ).toMap

    val sinkDf = if (plan.sinkOptions.isDefined) {
      ForeignKeyUtil.getDataFramesWithForeignKeys(plan.sinkOptions.get, generatedDataForeachTask)
    } else {
      generatedDataForeachTask
    }
    sinkDf
  }

  private def pushDataToSinks(executableTasks: List[(TaskSummary, Task)], sinkDf: Map[String, DataFrame]): Unit = {
    val sinkFactory = new SinkFactory(connectionConfigsByName, applicationType)
    val stepByDataSourceName = executableTasks.flatMap(task =>
      task._2.steps.map(s => (getDataSourceName(task._1, s), s))
    ).toMap

    sinkDf.foreach(df => {
      val dataSourceName = df._1.split("\\.").head
      val step = stepByDataSourceName(df._1)
      sinkFactory.pushToSink(df._2, dataSourceName, step, flagsConfig.enableCount)

      if (applicationType.equalsIgnoreCase(ADVANCED_APPLICATION) && flagsConfig.enableRecordTracking) {
        val format = connectionConfigsByName(dataSourceName)(FORMAT)
        recordTrackingFactory.trackRecords(df._2, dataSourceName, format, step)
      } else if (applicationType.equalsIgnoreCase(BASIC_APPLICATION) && flagsConfig.enableRecordTracking) {
        LOGGER.warn(s"Please upgrade from the free plan to paid plan to enable record tracking. More details here: $DATA_CATERER_SITE_PRICING")
      }
    })
  }

  private def getDataSourceName(taskSummary: TaskSummary, step: Step): String = {
    s"${taskSummary.dataSourceName}.${step.name}"
  }

  private def getCountPerStep(tasks: List[Task], faker: Faker): List[(String, Long)] = {
    tasks.flatMap(task => {
      task.steps.map(step => {
        val stepName = s"${task.name}_${step.name}"
        (stepName, getRecordCount(step.count, faker))
      })
    })
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
