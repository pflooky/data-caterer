package com.github.pflooky.datagen.core.generator.result

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.github.pflooky.datagen.core.config.{FoldersConfig, MetadataConfig}
import com.github.pflooky.datagen.core.model.{DataSourceResult, DataSourceResultSummary, Field, Plan, Step, StepResultSummary, Task, TaskResultSummary}
import com.github.pflooky.datagen.core.util.FileUtil.writeStringToFile
import com.github.pflooky.datagen.core.util.ObjectMapperUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

class DataGenerationResultWriter(metadataConfig: MetadataConfig, foldersConfig: FoldersConfig)(implicit sparkSession: SparkSession) {

  private lazy val LOGGER = Logger.getLogger(getClass.getName)
  private lazy val OBJECT_MAPPER = ObjectMapperUtil.jsonObjectMapper

  def writeResult(plan: Plan, generationResult: List[DataSourceResult]): Unit = {
    OBJECT_MAPPER.setSerializationInclusion(Include.NON_ABSENT)
    val (stepSummary, taskSummary, dataSourceSummary) = getSummaries(generationResult)
    val fileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    fileSystem.setWriteChecksum(false)

    val currentTime = new DateTime().toString(ISODateTimeFormat.basicDateTimeNoMillis())
    val fileName = s"generated-data-summary_${plan.name.replaceAll(" ", "_")}_$currentTime.json"
    val filePath = s"${foldersConfig.generatedDataResultsFolderPath}/$fileName"
    LOGGER.info(s"Writing data generation summary to file, file-path=$filePath")
//    val fileContent = OBJECT_MAPPER.writeValueAsString((plan, stepSummary, taskSummary, dataSourceSummary))
    val htmlWriter = new ResultHtmlWriter()
    val htmlOverviewContent = htmlWriter.overview(plan, stepSummary, taskSummary, dataSourceSummary)

//    writeStringToFile(fileSystem, filePath, fileContent)
    writeStringToFile(fileSystem, s"${foldersConfig.generatedDataResultsFolderPath}/index.html", htmlWriter.index.toString())
    writeStringToFile(fileSystem, s"${foldersConfig.generatedDataResultsFolderPath}/overview.html", htmlOverviewContent.toString())
    writeStringToFile(fileSystem, s"${foldersConfig.generatedDataResultsFolderPath}/navbar.html", htmlWriter.navBarDetails.toString())

    writeStringToFile(fileSystem, s"${foldersConfig.generatedDataResultsFolderPath}/plan.html", htmlWriter.planDetails(plan).toString())
    writeStringToFile(fileSystem, s"${foldersConfig.generatedDataResultsFolderPath}/tasks.html", htmlWriter.taskDetails(taskSummary).toString())
    writeStringToFile(fileSystem, s"${foldersConfig.generatedDataResultsFolderPath}/steps.html", htmlWriter.stepDetails(stepSummary).toString())
    writeStringToFile(fileSystem, s"${foldersConfig.generatedDataResultsFolderPath}/data-source.html", htmlWriter.dataSourceDetails(stepSummary.flatMap(_.dataSourceResults)).toString())
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

