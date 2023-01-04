package com.github.pflooky.datagen.core.generator

import com.github.pflooky.datagen.core.model.{Plan, Step, Task, TaskSummary}
import com.github.pflooky.datagen.core.parser.PlanParser
import com.github.pflooky.datagen.core.sink.SinkFactory
import com.github.pflooky.datagen.core.util.{ForeignKeyUtil, SparkProvider}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

class DataGeneratorProcessor extends SparkProvider {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def generateData(): Unit = {
    val plan = PlanParser.parsePlan(planFilePath)
    val plannedTasks = plan.tasks.filter(_.enabled).map(t => (t.name, t))

    val tasks = PlanParser.parseTasks(taskFolderPath)
    val tasksByName = tasks.map(t => (t.name, t)).toMap

    val executableTasks = plannedTasks.map(pt => (pt._2, tasksByName(pt._1)))
    val sinkDf = getAllStepDf(plan, executableTasks)

    pushDataToSinks(executableTasks, sinkDf)
  }

  private def getAllStepDf(plan: Plan, executableTasks: List[(TaskSummary, Task)]): Map[String, DataFrame] = {
    val dataGeneratorFactory = new DataGeneratorFactory
    val generatedDataForeachTask = executableTasks.flatMap(task =>
      task._2.steps.map(s => (getSinkName(task._1, s), dataGeneratorFactory.generateDataForStep(s, task._1.sinkName)))
    ).toMap

    val sinkDf = if (plan.sinkOptions.isDefined) {
      ForeignKeyUtil.getDataFramesWithForeignKeys(plan.sinkOptions.get, generatedDataForeachTask)
    } else {
      generatedDataForeachTask
    }
    sinkDf
  }

  private def pushDataToSinks(executableTasks: List[(TaskSummary, Task)], sinkDf: Map[String, DataFrame]): Unit = {
    val sinkFactory = new SinkFactory(executableTasks, connectionConfigs)
    val stepOptionsBySink = executableTasks.flatMap(task =>
      task._2.steps.map(s => (getSinkName(task._1, s), s.options))
    ).toMap

    sinkDf.foreach(df => {
      val sinkName = df._1.split("\\.").head
      LOGGER.info(s"Pushing data to sink, sink-name=$sinkName")
      sinkFactory.pushToSink(df._2, sinkName, stepOptionsBySink(df._1))
      LOGGER.info(s"Successfully pushed data to sink, sink-name=$sinkName")
    })
  }

  private def getSinkName(taskSummary: TaskSummary, step: Step): String = {
    s"${taskSummary.sinkName}.${step.name}"
  }
}
