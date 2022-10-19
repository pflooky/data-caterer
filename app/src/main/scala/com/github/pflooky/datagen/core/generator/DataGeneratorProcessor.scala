package com.github.pflooky.datagen.core.generator

import com.github.pflooky.datagen.core.parser.PlanParser
import com.github.pflooky.datagen.core.sink.SinkFactory
import com.github.pflooky.datagen.core.util.{ForeignKeyUtil, SparkProvider}
import org.apache.log4j.Logger

class DataGeneratorProcessor extends SparkProvider {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def generateData(): Unit = {
    val plan = PlanParser.parsePlan(planFilePath)
    val plannedTasks = plan.tasks.filter(_.enabled).map(t => (t.name, t))

    val tasks = PlanParser.parseTasks(taskFolderPath)
    val tasksByName = tasks.map(t => (t.name, t)).toMap

    val executableTasks = plannedTasks.map(pt => (pt._2, tasksByName(pt._1)))
    val dataGeneratorFactory = new DataGeneratorFactory
    val sinkFactory = new SinkFactory(executableTasks, connectionConfigs)

    val stepOptionsBySink = executableTasks.flatMap(task =>
      task._2.steps.map(s => (s"${task._1.sinkName}.${s.name}", s.options))
    ).toMap
    val generatedDataForeachTask = executableTasks.flatMap(task =>
      task._2.steps.map(s => (s"${task._1.sinkName}.${s.name}", dataGeneratorFactory.generateDataForStep(s, task._1.sinkName)))
    ).toMap

    val foreignKeyDfs = if (plan.sinkOptions.isDefined) {
      ForeignKeyUtil.getDataFramesWithForeignKeys(plan.sinkOptions.get, generatedDataForeachTask)
    } else {
      generatedDataForeachTask
    }

    foreignKeyDfs.foreach(df => {
      val sinkName = df._1.split("\\.").head
      LOGGER.info(s"Pushing data to sink, sink-name=$sinkName")
      sinkFactory.pushToSink(df._2, sinkName, stepOptionsBySink(df._1))
      LOGGER.info(s"Successfully pushed data to sink, sink-name=$sinkName")
    })
  }

}
