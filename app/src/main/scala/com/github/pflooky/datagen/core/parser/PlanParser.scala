package com.github.pflooky.datagen.core.parser

import com.github.pflooky.datagen.core.exception.TaskParseException
import com.github.pflooky.datagen.core.model.Constants.ONE_OF
import com.github.pflooky.datagen.core.model.{Plan, Schema, Step, Task}
import com.github.pflooky.datagen.core.util.FileUtil.{CLOUD_STORAGE_REGEX, getFileContentFromFileSystem, isCloudStoragePath}
import com.github.pflooky.datagen.core.util.{FileUtil, ObjectMapperUtil}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.util.{Failure, Success, Try}

object PlanParser {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OBJECT_MAPPER = ObjectMapperUtil.yamlObjectMapper

  def parsePlan(planFilePath: String)(implicit sparkSession: SparkSession): Plan = {
    val parsedPlan = if (isCloudStoragePath(planFilePath)) {
      val fileContent = getFileContentFromFileSystem(FileSystem.get(sparkSession.sparkContext.hadoopConfiguration), planFilePath)
      OBJECT_MAPPER.readValue(fileContent, classOf[Plan])
    } else {
      val planFile = FileUtil.getFile(planFilePath)
      OBJECT_MAPPER.readValue(planFile, classOf[Plan])
    }
    LOGGER.info(s"Found plan file and parsed successfully, plan-file-path=$planFilePath, plan-name=${parsedPlan.name}, plan-description=${parsedPlan.description}")
    parsedPlan
  }

  def parseTasks(taskFolderPath: String)(implicit sparkSession: SparkSession): Array[Task] = {
    if (isCloudStoragePath(taskFolderPath)) {
      val fileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      val allTaskFiles = fileSystem.listFiles(new Path(taskFolderPath), true)

      val taskArray = scala.collection.mutable.ArrayBuffer[Task]()
      while (allTaskFiles.hasNext) {
        val taskFileName = allTaskFiles.next().getPath.toString
        val taskFileContent = getFileContentFromFileSystem(fileSystem, taskFileName)
        val task = convertTaskNumbersToString(Try(OBJECT_MAPPER.readValue(taskFileContent, classOf[Task])), taskFileName)
        taskArray.append(task)
      }
      taskArray.toArray
    } else {
      val taskFolder = FileUtil.getDirectory(taskFolderPath)
      getTaskFiles(taskFolder).map(parseTask)
    }
  }

  private def getTaskFiles(taskFolder: File): Array[File] = {
    if (!taskFolder.isDirectory) {
      LOGGER.warn(s"Task folder is not a directory, unable to list files, path=${taskFolder.getPath}")
      Array()
    } else {
      val current = taskFolder.listFiles().filter(_.getName.endsWith(".yaml"))
      current ++ taskFolder.listFiles
        .filter(_.isDirectory)
        .flatMap(getTaskFiles)
    }
  }

  private def parseTask(taskFile: File): Task = {
    val tryParseTask = Try(OBJECT_MAPPER.readValue(taskFile, classOf[Task]))
    convertTaskNumbersToString(tryParseTask, taskFile.getAbsolutePath)
  }

  private def convertTaskNumbersToString(tryParseTask: Try[Task], taskFileName: String): Task = {
    val parsedTask = tryParseTask match {
      case Success(x) => x
      case Failure(exception) => throw new TaskParseException(taskFileName, exception)
    }
    val stringSteps = parsedTask.steps.map(step => {
      val countPerColGenerator = step.count.perColumn.map(perColumnCount => {
        val generator = perColumnCount.generator.map(gen => gen.copy(options = toStringValues(gen.options)))
        perColumnCount.copy(generator = generator)
      })
      val countGenerator = step.count.generator.map(gen => gen.copy(options = toStringValues(gen.options)))
      val mappedSchema = schemaToString(step.schema)
      step.copy(
        count = step.count.copy(perColumn = countPerColGenerator, generator = countGenerator),
        schema = mappedSchema
      )
    })
    parsedTask.copy(steps = stringSteps)
  }

  private def schemaToString(schema: Schema): Schema = {
    val mappedFields = schema.fields.map(fields => {
      fields.map(field => {
        if (field.generator.isDefined && field.generator.get.`type` != ONE_OF) {
          val fieldGenOpt = toStringValues(field.generator.get.options)
          field.copy(generator = Some(field.generator.get.copy(options = fieldGenOpt)))
        } else {
          field.copy(schema = field.schema.map(schemaToString))
        }
      })
    })
    schema.copy(fields = mappedFields)
  }

  private def toStringValues(options: Map[String, Any]): Map[String, Any] = {
    options.map(x => (x._1, x._2.toString))
  }
}
