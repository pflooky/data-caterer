package com.github.pflooky.datagen.core.generator.result

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.github.pflooky.datagen.core.model.{DataSourceResult, DataSourceResultSummary, Field, FlagsConfig, FoldersConfig, MetadataConfig, Plan, Step, StepResultSummary, Task, TaskResultSummary}
import com.github.pflooky.datagen.core.util.FileUtil.writeStringToFile
import com.github.pflooky.datagen.core.util.ObjectMapperUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.xml.Node

class DataGenerationResultWriter(metadataConfig: MetadataConfig, foldersConfig: FoldersConfig, flagsConfig: FlagsConfig)(implicit sparkSession: SparkSession) {

  private lazy val LOGGER = Logger.getLogger(getClass.getName)
  private lazy val OBJECT_MAPPER = ObjectMapperUtil.jsonObjectMapper

  def writeResult(plan: Plan, generationResult: List[DataSourceResult]): Unit = {
    OBJECT_MAPPER.setSerializationInclusion(Include.NON_ABSENT)
    val (stepSummary, taskSummary, dataSourceSummary) = getSummaries(generationResult)
    val fileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    fileSystem.setWriteChecksum(false)

    LOGGER.info(s"Writing data generation summary to html files, folder-path=${foldersConfig.generatedDataResultsFolderPath}")
    val htmlWriter = new ResultHtmlWriter()
    val htmlOverviewContent = htmlWriter.overview(plan, stepSummary, taskSummary, dataSourceSummary, flagsConfig)
    val fileWriter = writeToFile(fileSystem, foldersConfig.generatedDataResultsFolderPath) _

    fileWriter("index.html", htmlWriter.index)
    fileWriter("overview.html", htmlOverviewContent)
    fileWriter("navbar.html", htmlWriter.navBarDetails)

    fileWriter("tasks.html", htmlWriter.taskDetails(taskSummary))
    fileWriter("steps.html", htmlWriter.stepDetails(stepSummary))
    fileWriter("data-sources.html", htmlWriter.dataSourceDetails(stepSummary.flatMap(_.dataSourceResults)))
  }

  private def writeToFile(fileSystem: FileSystem, folderPath: String)(fileName: String, content: Node): Unit = {
    writeStringToFile(fileSystem, s"$folderPath/$fileName", content.toString())
  }

  private def getSummaries(generationResult: List[DataSourceResult]): (List[StepResultSummary], List[TaskResultSummary], List[DataSourceResultSummary]) = {
    val resultByStep = generationResult.groupBy(_.step).map(getResultSummary).toList
    val resultByTask = generationResult.groupBy(_.task).map(getResultSummary).toList
    val resultByDataSource = generationResult.groupBy(_.name).map(getResultSummary).toList
    (resultByStep, resultByTask, resultByDataSource)
  }

  private def getResultSummary(result: (Step, List[DataSourceResult])): StepResultSummary = {
    val (totalRecords, isSuccess, _, _) = summariseDataSourceResult(result._2)
    StepResultSummary(result._1, totalRecords, isSuccess, result._2)
  }

  private def getResultSummary(result: (Task, List[DataSourceResult])): TaskResultSummary = {
    val (totalRecords, isSuccess, _, _) = summariseDataSourceResult(result._2)
    val stepResults = result._1.steps.map(step => getResultSummary((step, result._2.filter(_.step == step))))
    TaskResultSummary(result._1, totalRecords, isSuccess, stepResults)
  }

  private def getResultSummary(result: (String, List[DataSourceResult])): DataSourceResultSummary = {
    val (totalRecords, isSuccess, _, _) = summariseDataSourceResult(result._2)
    DataSourceResultSummary(result._1, totalRecords, isSuccess, result._2)
  }

  private def summariseDataSourceResult(dataSourceResults: List[DataSourceResult]): (Long, Boolean, List[String], List[Field]) = {
    val totalRecords = dataSourceResults.map(_.sinkResult.count).sum
    val isSuccess = dataSourceResults.forall(_.sinkResult.isSuccess)
    val sample = dataSourceResults.flatMap(_.sinkResult.sample).take(metadataConfig.numSinkSamples)
    val fieldMetadata = dataSourceResults.flatMap(_.sinkResult.generatedMetadata)
      .groupBy(_.name)
      .map(field => {
        val metadataList = field._2.map(_.generator.map(gen => gen.options).getOrElse(Map()))
        //TODO combine the metadata from each batch together to show summary
        field._2.head
      }).toList
    (totalRecords, isSuccess, sample, fieldMetadata)
  }

}

