package com.github.pflooky.datagen.core.parser

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.pflooky.datagen.core.exception.{PlanFileNotFoundException, TaskParseException}
import com.github.pflooky.datagen.core.model.{Plan, Task}
import org.apache.log4j.Logger

import java.io.File
import scala.util.{Failure, Success, Try}

object PlanParser {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
  OBJECT_MAPPER.registerModule(DefaultScalaModule)

  def parsePlan(planFilePath: String): Plan = {
    val planFile = new File(planFilePath)
    val parsedPlan = if (!planFile.exists()) {
      val mainPlanFile = getClass.getResource(planFilePath)
      OBJECT_MAPPER.readValue(mainPlanFile, classOf[Plan])
    } else {
      throw new PlanFileNotFoundException(s"Failed to find plan file, plan-file-path=$planFilePath")
    }
    LOGGER.info(s"Found plan file and parsed successfully, plan-file-path=$planFilePath, plan-name=${parsedPlan.name}, plan-description=${parsedPlan.description}")
    parsedPlan
  }

  def parseTasks(taskFolderPath: String): Array[Task] = {
    var taskFolder = new File(taskFolderPath)
    if (!taskFolder.isDirectory) {
      taskFolder = new File(getClass.getResource(taskFolderPath).getFile)
//      return OBJECT_MAPPER.readValue(mainPlanFile, classOf[Plan])
//      throw TaskFolderNotDirectoryException(taskFolderPath)
    }
    val parsedTasks = getTaskFiles(taskFolder).map(parseTask)
    parsedTasks
  }

  private def getTaskFiles(taskFolder: File): Array[File] = {
    val current = taskFolder.listFiles().filter(_.getName.endsWith("-task.yaml"))
    current ++ taskFolder.listFiles
      .filter(_.isDirectory)
      .flatMap(getTaskFiles)
  }

  private def parseTask(taskFile: File): Task = {
    val tryParseTask = Try(OBJECT_MAPPER.readValue(taskFile, classOf[Task]))
    tryParseTask match {
      case Success(x) => x
      case Failure(exception) => throw new TaskParseException(taskFile.getName, exception)
    }
  }

}
