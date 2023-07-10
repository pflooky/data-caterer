package com.github.pflooky.datagen.core.generator

import com.github.pflooky.datagen.core.generator.track.RecordTrackingProcessor
import com.github.pflooky.datagen.core.model.Constants.FORMAT
import com.github.pflooky.datagen.core.model.{ForeignKeyRelation, Plan, Step, Task, TaskSummary}
import com.github.pflooky.datagen.core.parser.PlanParser
import com.github.pflooky.datagen.core.sink.SinkFactory
import com.github.pflooky.datagen.core.util.{ForeignKeyUtil, SparkProvider}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

class DataGeneratorProcessor extends SparkProvider {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val recordTrackingFactory = new RecordTrackingProcessor(s"${foldersConfig.baseFolderPath}/${foldersConfig.recordTrackingFolderPath}")

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

    if (flagsConfig.enableDeleteGeneratedRecords) {
      deleteGeneratedRecords(plan, stepsByName, summaryWithTask)
    } else if (flagsConfig.enableGenerateData) {
      LOGGER.info(s"Following tasks are enabled and will be executed: num-tasks=${summaryWithTask.size}, tasks=($summaryWithTask)")
      summaryWithTask.foreach(t => LOGGER.info(s"Enabled task details: ${t._2.toTaskDetailString}"))
      val sinkDf = getAllStepDf(plan, summaryWithTask)
      pushDataToSinks(summaryWithTask, sinkDf)
    } else {
      LOGGER.info("Data generation is disabled")
    }
  }

  private def deleteGeneratedRecords(plan: Plan, stepsByName: Map[String, Step], summaryWithTask: List[(TaskSummary, Task)]): Unit = {
    if (plan.sinkOptions.isDefined && plan.sinkOptions.get.foreignKeys.nonEmpty) {
      val deleteOrder = ForeignKeyUtil.getDeleteOrder(plan.sinkOptions.get.foreignKeys)
      deleteOrder.foreach(foreignKeyName => {
        val foreignKeyRelation = ForeignKeyRelation.fromString(foreignKeyName)
        val connectionConfig = connectionConfigsByName(foreignKeyRelation.dataSource)
        val format = connectionConfig(FORMAT)
        val step = stepsByName(foreignKeyRelation.step)
        recordTrackingFactory.deleteRecords(foreignKeyRelation.dataSource, format, step.options, connectionConfig)
      })
    } else {
      summaryWithTask.foreach(task => {
        task._2.steps.foreach(step => {
          val connectionConfig = connectionConfigsByName(task._1.dataSourceName)
          val format = connectionConfig(FORMAT)
          recordTrackingFactory.deleteRecords(task._1.dataSourceName, format, step.options, connectionConfig)
        })
      })
    }
  }

  private def getAllStepDf(plan: Plan, executableTasks: List[(TaskSummary, Task)]): Map[String, DataFrame] = {
    val dataGeneratorFactory = new DataGeneratorFactory(plan.sinkOptions.flatMap(_.seed), plan.sinkOptions.flatMap(_.locale))
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
    val sinkFactory = new SinkFactory(connectionConfigsByName)
    val stepByDataSourceName = executableTasks.flatMap(task =>
      task._2.steps.map(s => (getDataSourceName(task._1, s), s))
    ).toMap

    sinkDf.foreach(df => {
      val dataSourceName = df._1.split("\\.").head
      val step = stepByDataSourceName(df._1)
      sinkFactory.pushToSink(df._2, dataSourceName, step, flagsConfig.enableCount)
      if (flagsConfig.enableRecordTracking) {
        val format = connectionConfigsByName(dataSourceName)(FORMAT)
        recordTrackingFactory.trackRecords(df._2, dataSourceName, format, step)
      }
    })
  }

  private def getDataSourceName(taskSummary: TaskSummary, step: Step): String = {
    s"${taskSummary.dataSourceName}.${step.name}"
  }
}
