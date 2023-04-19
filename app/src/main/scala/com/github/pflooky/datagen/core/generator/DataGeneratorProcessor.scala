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
    val enabledPlannedTasks = plan.tasks.filter(_.enabled)
    val enabledTaskDetails = enabledPlannedTasks.map(x => s"name=${x.name} => sink=${x.sinkName}").mkString(", ")
    LOGGER.info(s"Following tasks are enabled and will be executed: num-tasks=${enabledPlannedTasks.size}, tasks: ($enabledTaskDetails)")
    val enabledTaskMap = enabledPlannedTasks.map(t => (t.name, t))

    val tasks = PlanParser.parseTasks(taskFolderPath)
    val tasksByName = tasks.map(t => (t.name, t)).toMap
    val executableTasks = enabledTaskMap.map(pt => (pt._2, tasksByName(pt._1)))
    executableTasks.foreach(t => LOGGER.info(s"Enabled task details: ${t._2.toTaskDetailString}"))
    val sinkDf = getAllStepDf(plan, executableTasks)

    pushDataToSinks(executableTasks, sinkDf)
  }

  def generateData(plan: Plan, tasks: List[Task]): Unit = {
    val tasksByName = tasks.map(t => (t.name, t)).toMap
    val summaryWithTask = plan.tasks.map(t => (t, tasksByName(t.name)))
    val sinkDf = getAllStepDf(plan, summaryWithTask)
    pushDataToSinks(summaryWithTask, sinkDf)
  }

  private def getAllStepDf(plan: Plan, executableTasks: List[(TaskSummary, Task)]): Map[String, DataFrame] = {
    val dataGeneratorFactory = new DataGeneratorFactory(plan.sinkOptions.flatMap(_.seed))
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
      sinkFactory.pushToSink(df._2, sinkName, stepOptionsBySink(df._1), enableCount)
    })
  }

  private def getSinkName(taskSummary: TaskSummary, step: Step): String = {
    s"${taskSummary.sinkName}.${step.name}"
  }
}
