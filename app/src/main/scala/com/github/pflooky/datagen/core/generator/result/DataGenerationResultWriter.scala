package com.github.pflooky.datagen.core.generator.result

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.github.pflooky.datacaterer.api.model.{Field, FlagsConfig, FoldersConfig, MetadataConfig, Plan, Step, Task}
import com.github.pflooky.datagen.core.listener.SparkRecordListener
import com.github.pflooky.datagen.core.model.Constants.{REPORT_DATA_SOURCES_HTML, REPORT_FIELDS_HTML, REPORT_HOME_HTML, REPORT_VALIDATIONS_HTML}
import com.github.pflooky.datagen.core.model.{DataSourceResult, DataSourceResultSummary, StepResultSummary, TaskResultSummary, ValidationConfigResult}
import com.github.pflooky.datagen.core.util.FileUtil.writeStringToFile
import com.github.pflooky.datagen.core.util.ObjectMapperUtil
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.xml.Node

class DataGenerationResultWriter(metadataConfig: MetadataConfig, foldersConfig: FoldersConfig, flagsConfig: FlagsConfig)(implicit sparkSession: SparkSession) {

  private lazy val LOGGER = Logger.getLogger(getClass.getName)
  private lazy val OBJECT_MAPPER = ObjectMapperUtil.jsonObjectMapper

  def writeResult(
                   plan: Plan,
                   generationResult: List[DataSourceResult],
                   validationResults: List[ValidationConfigResult],
                   sparkRecordListener: SparkRecordListener
                 ): Unit = {
    OBJECT_MAPPER.setSerializationInclusion(Include.NON_ABSENT)
    val (stepSummary, taskSummary, dataSourceSummary) = getSummaries(generationResult)
    val fileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    fileSystem.setWriteChecksum(false)

    LOGGER.info(s"Writing data generation summary to HTML files, folder-path=${foldersConfig.generatedReportsFolderPath}")
    val htmlWriter = new ResultHtmlWriter()
    val fileWriter = writeToFile(fileSystem, foldersConfig.generatedReportsFolderPath) _

    try {
      fileSystem.copyFromLocalFile(
        new Path(getClass.getResource("/report/css/main.css").toURI),
        new Path(s"${foldersConfig.generatedReportsFolderPath}/main.css")
      )
      fileSystem.copyFromLocalFile(
        new Path(getClass.getResource("/report/logo/data_catering_transparent.svg").toURI),
        new Path(s"${foldersConfig.generatedReportsFolderPath}/data_catering_transparent.svg")
      )
      fileWriter(REPORT_HOME_HTML, htmlWriter.index(plan, stepSummary, taskSummary, dataSourceSummary, validationResults, flagsConfig, sparkRecordListener))

      fileWriter("tasks.html", htmlWriter.taskDetails(taskSummary))
      fileWriter(REPORT_FIELDS_HTML, htmlWriter.stepDetails(stepSummary))
      fileWriter(REPORT_DATA_SOURCES_HTML, htmlWriter.dataSourceDetails(stepSummary.flatMap(_.dataSourceResults)))
      fileWriter(REPORT_VALIDATIONS_HTML, htmlWriter.validations(validationResults))
    } catch {
      case ex: Exception =>
        LOGGER.error("Failed to write data generation summary to HTML files", ex)
    }
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
    val sample = dataSourceResults.flatMap(_.sinkResult.sample).take(metadataConfig.numGeneratedSamples)
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

