package com.github.pflooky.datagen.core.generator.plan

import com.github.pflooky.datagen.core.model.{Plan, SinkOptions, Task, TaskSummary}
import com.github.pflooky.datagen.core.util.FileUtil.writeStringToFile
import com.github.pflooky.datagen.core.util.ObjectMapperUtil
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import java.io.File

object PlanGenerator {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OBJECT_MAPPER = ObjectMapperUtil.yamlObjectMapper

  def writePlanAndTasksToFiles(tasks: List[(String, Task)], foreignKeys: Map[String, List[String]], baseFolderPath: String)
                              (implicit sparkSession: SparkSession): (Plan, List[Task]) = {
    val fileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    fileSystem.setWriteChecksum(false)
    val plan = writePlanToFile(tasks, foreignKeys, s"$baseFolderPath/plan", fileSystem)
    writeTasksToFiles(tasks, s"$baseFolderPath/task", fileSystem)
    fileSystem.close()
    (plan, tasks.map(_._2))
  }

  private def writePlanToFile(tasks: List[(String, Task)], foreignKeys: Map[String, List[String]], planFolder: String, fileSystem: FileSystem)
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
}
