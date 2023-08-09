package com.github.pflooky.datagen.core.generator

import com.github.pflooky.datagen.core.generator.delete.DeleteRecordProcessor
import com.github.pflooky.datagen.core.generator.track.RecordTrackingProcessor
import com.github.pflooky.datagen.core.model.Constants.{ADVANCED_APPLICATION, BASIC_APPLICATION, DATA_CATERER_SITE_PRICING, FORMAT}
import com.github.pflooky.datagen.core.model.{Plan, Step, Task, TaskSummary}
import com.github.pflooky.datagen.core.parser.PlanParser
import com.github.pflooky.datagen.core.sink.SinkFactory
import com.github.pflooky.datagen.core.util.GeneratorUtil.getRecordCount
import com.github.pflooky.datagen.core.util.{ForeignKeyUtil, SparkProvider, UniqueFieldUtil}
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

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

    (flagsConfig.enableGenerateData, flagsConfig.enableDeleteGeneratedRecords, applicationType) match {
      case (true, enableDelete, _) =>
        if (enableDelete) {
          LOGGER.warn("Both enableGenerateData and enableDeleteGeneratedData are true. Please only enable one at a time. Will continue with generating data")
        }
        if (LOGGER.isDebugEnabled) {
          LOGGER.debug(s"Following tasks are enabled and will be executed: num-tasks=${summaryWithTask.size}, tasks=($summaryWithTask)")
          summaryWithTask.foreach(t => LOGGER.debug(s"Enabled task details: ${t._2.toTaskDetailString}"))
        }
        val stepNames = summaryWithTask.map(t => s"task=${t._2.name}, steps=${t._2.steps.map(_.name).mkString(",")}").mkString("||")
        LOGGER.info(s"Following tasks are enabled and will be executed: num-tasks=${summaryWithTask.size}, tasks=$stepNames")
        //TODO batch up 5000 (configurable number) records total across all steps and progressively push to sinks
        /**
         * can do the following for batching the data:
         * 1. calculate the total counts across all steps
         * 2. create accumulators to keep track of count for each step
         */
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

  private def getAllStepDf(plan: Plan, executableTasks: List[(TaskSummary, Task)]): List[(String, DataFrame)] = {
    val faker = getDataFaker(plan)
    val dataGeneratorFactory = new DataGeneratorFactory(faker)
    val uniqueFieldUtil = new UniqueFieldUtil(executableTasks)

    val generatedDataForeachTask = executableTasks.flatMap(task =>
      task._2.steps.map(s => {
        val dataSourceName = getDataSourceName(task._1, s)
        val genDf = dataGeneratorFactory.generateDataForStep(s, task._1.dataSourceName)

        val primaryKeys = s.gatherPrimaryKeys
        val primaryDf = if (primaryKeys.nonEmpty) {
          genDf.dropDuplicates(primaryKeys)
        } else genDf

        val df = if (s.hasUniqueFields) {
          uniqueFieldUtil.getUniqueFieldsValues(dataSourceName, primaryDf)
        } else primaryDf
        (dataSourceName, df)
      })
    ).toMap

    val sinkDf = if (plan.sinkOptions.isDefined && plan.sinkOptions.get.foreignKeys.nonEmpty) {
      ForeignKeyUtil.getDataFramesWithForeignKeys(plan, generatedDataForeachTask)
    } else {
      generatedDataForeachTask.toList
    }
    sinkDf
  }

  private def pushDataToSinks(executableTasks: List[(TaskSummary, Task)], sinkDf: List[(String, DataFrame)]): Unit = {
    val sinkFactory = new SinkFactory(connectionConfigsByName, applicationType)
    val stepByDataSourceName = executableTasks.flatMap(task =>
      task._2.steps.map(s => (getDataSourceName(task._1, s), s))
    ).toMap

    sinkDf.foreach(df => {
      val dataSourceName = df._1.split("\\.").head
      val step = stepByDataSourceName(df._1)
      sinkFactory.pushToSink(df._2, dataSourceName, step, flagsConfig)

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
