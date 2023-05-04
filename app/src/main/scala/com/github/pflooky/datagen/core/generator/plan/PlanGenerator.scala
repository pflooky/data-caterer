package com.github.pflooky.datagen.core.generator.plan

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.pflooky.datagen.core.model.{Plan, SinkOptions, Task, TaskSummary}
import org.apache.log4j.Logger
import org.joda.time.DateTime

import java.io.File

object PlanGenerator {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
  OBJECT_MAPPER.registerModule(DefaultScalaModule)
  OBJECT_MAPPER.setSerializationInclusion(Include.NON_ABSENT)

  def writePlanAndTasksToFiles(tasks: List[(String, Task)], foreignKeys: Map[String, List[String]], baseFolderPath: String): (Plan, List[Task]) = {
    val baseGeneratedFolder = new File(s"$baseFolderPath/generated")
    val planFolder = new File(s"${baseGeneratedFolder.getPath}/plan")
    val taskFolder = new File(s"${baseGeneratedFolder.getPath}/task")
    baseGeneratedFolder.mkdirs()
    planFolder.mkdirs()
    taskFolder.mkdirs()

    val plan = writePlanToFile(tasks, foreignKeys, planFolder)
    writeTasksToFiles(tasks, taskFolder)
    (plan, tasks.map(_._2))
  }

  private def writePlanToFile(tasks: List[(String, Task)], foreignKeys: Map[String, List[String]], planFolder: File): Plan = {
    val currentTime = new DateTime().toString("yyyy-MM-dd_HH:mm")
    val taskSummary = tasks.map(t => TaskSummary(t._2.name, t._1))
    val plan = Plan(s"plan_$currentTime", "Generated plan", taskSummary, Some(SinkOptions(None, None, foreignKeys)))
    val generatedPlanFilePath = new File(s"$planFolder/plan_$currentTime.yaml")
    LOGGER.info(s"Writing plan to file, plan=${plan.name}, num-tasks=${plan.tasks.size}, file-path=$generatedPlanFilePath")
    generatedPlanFilePath.createNewFile()
    OBJECT_MAPPER.writeValue(generatedPlanFilePath, plan)
    plan
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
