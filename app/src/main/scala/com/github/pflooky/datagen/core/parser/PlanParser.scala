package com.github.pflooky.datagen.core.parser

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.pflooky.datagen.core.exception.TaskParseException
import com.github.pflooky.datagen.core.model.{Plan, Task}

import java.io.File
import scala.util.{Failure, Success, Try}

object PlanParser {

  private val objectMapper = new ObjectMapper(new YAMLFactory())
  objectMapper.registerModule(DefaultScalaModule)

  def parsePlan(planFilePath: String): Plan = {
    val planFile = new File(planFilePath)
    val parsedPlan = if (!planFile.exists()) {
      //try under src/main/resources
      val mainPlanFile = getClass.getResource(planFilePath)
      objectMapper.readValue(mainPlanFile, classOf[Plan])
//      throw PlanFileNotFoundException(planFilePath)
    } else {
      objectMapper.readValue(planFile, classOf[Plan])
    }
    parsedPlan
  }

  def parseTasks(taskFolderPath: String): Array[Task] = {
    var taskFolder = new File(taskFolderPath)
    if (!taskFolder.isDirectory) {
      taskFolder = new File(getClass.getResource(taskFolderPath).getFile)
//      return objectMapper.readValue(mainPlanFile, classOf[Plan])
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
    val tryParseTask = Try(objectMapper.readValue(taskFile, classOf[Task]))
    tryParseTask match {
      case Success(x) => x
      case Failure(exception) => throw new TaskParseException(taskFile.getName, exception)
    }
  }

}
