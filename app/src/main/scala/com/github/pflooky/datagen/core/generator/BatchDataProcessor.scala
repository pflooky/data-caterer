package com.github.pflooky.datagen.core.generator

import com.github.pflooky.datacaterer.api.model.Constants.FORMAT
import com.github.pflooky.datacaterer.api.model.{ExpressionValidation, FlagsConfig, FoldersConfig, GenerationConfig, GroupByValidation, MetadataConfig, Plan, Task, TaskSummary, UpstreamDataSourceValidation, ValidationConfiguration}
import com.github.pflooky.datagen.core.generator.track.RecordTrackingProcessor
import com.github.pflooky.datagen.core.model.Constants.{ADVANCED_APPLICATION, BASIC_APPLICATION, DATA_CATERER_SITE_PRICING, TRIAL_APPLICATION}
import com.github.pflooky.datagen.core.model.DataSourceResult
import com.github.pflooky.datagen.core.model.PlanImplicits.StepOps
import com.github.pflooky.datagen.core.sink.SinkFactory
import com.github.pflooky.datagen.core.util.GeneratorUtil.getDataSourceName
import com.github.pflooky.datagen.core.util.RecordCountUtil.calculateNumBatches
import com.github.pflooky.datagen.core.util.{ForeignKeyUtil, UniqueFieldsUtil}
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.Serializable
import java.time.LocalDateTime
import java.util.{Locale, Random}
import scala.util.{Failure, Success, Try}

class BatchDataProcessor(connectionConfigsByName: Map[String, Map[String, String]], foldersConfig: FoldersConfig,
                         metadataConfig: MetadataConfig, flagsConfig: FlagsConfig, generationConfig: GenerationConfig, applicationType: String)(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private lazy val sinkFactory = new SinkFactory(flagsConfig, metadataConfig, applicationType)

  def splitAndProcess(plan: Plan, executableTasks: List[(TaskSummary, Task)], optValidations: Option[List[ValidationConfiguration]])
                     (implicit sparkSession: SparkSession): List[DataSourceResult] = {
    val faker = getDataFaker(plan)
    val dataGeneratorFactory = new DataGeneratorFactory(faker)
    val uniqueFieldUtil = new UniqueFieldsUtil(executableTasks)
    val tasks = executableTasks.map(_._2)
    var (numBatches, trackRecordsPerStep) = calculateNumBatches(tasks, generationConfig)

    val dataSourceResults = (1 to numBatches).flatMap(batch => {
      val startTime = LocalDateTime.now()
      LOGGER.info(s"Starting batch, batch=$batch, num-batches=$numBatches")
      val generatedDataForeachTask = executableTasks.flatMap(task =>
        task._2.steps.filter(_.enabled).map(s => {
          val dataSourceStepName = getDataSourceName(task._1, s)
          val recordStepName = s"${task._2.name}_${s.name}"
          val stepRecords = trackRecordsPerStep(recordStepName)
          val startIndex = stepRecords.currentNumRecords
          val endIndex = stepRecords.currentNumRecords + stepRecords.numRecordsPerBatch

          val genDf = dataGeneratorFactory.generateDataForStep(s, task._1.dataSourceName, startIndex, endIndex)
          val df = if (s.gatherPrimaryKeys.nonEmpty && flagsConfig.enableUniqueCheck) uniqueFieldUtil.getUniqueFieldsValues(dataSourceStepName, genDf) else genDf

          if (!df.storageLevel.useMemory) df.cache()
          val dfRecordCount = if (flagsConfig.enableCount) df.count() else stepRecords.numRecordsPerBatch
          LOGGER.debug(s"Step record count for batch, batch=$batch, step-name=${s.name}, target-num-records=${stepRecords.numRecordsPerBatch}, actual-num-records=$dfRecordCount")
          trackRecordsPerStep = trackRecordsPerStep ++ Map(recordStepName -> stepRecords.copy(currentNumRecords = dfRecordCount))
          (dataSourceStepName, df)
        })
      ).toMap

      val sinkDf = plan.sinkOptions
        .map(_ => ForeignKeyUtil.getDataFramesWithForeignKeys(plan, generatedDataForeachTask))
        .getOrElse(generatedDataForeachTask.toList)
      val sinkResults = pushDataToSinks(executableTasks, sinkDf, batch, startTime, optValidations)
      sinkDf.foreach(_._2.unpersist())
      LOGGER.info(s"Finished batch, batch=$batch, num-batches=$numBatches")
      sinkResults
    }).toList
    LOGGER.debug(s"Completed all batches, num-batches=$numBatches")
    dataSourceResults
  }

  def pushDataToSinks(executableTasks: List[(TaskSummary, Task)], sinkDf: List[(String, DataFrame)], batchNum: Int,
                      startTime: LocalDateTime, optValidations: Option[List[ValidationConfiguration]]): List[DataSourceResult] = {
    val stepAndTaskByDataSourceName = executableTasks.flatMap(task =>
      task._2.steps.map(s => (getDataSourceName(task._1, s), (s, task._2)))
    ).toMap
    val dataSourcesUsedInValidation = getDataSourcesUsedInValidation(optValidations)

    sinkDf.map(df => {
      val dataSourceName = df._1.split("\\.").head
      val (step, task) = stepAndTaskByDataSourceName(df._1)
      val dataSourceConfig = connectionConfigsByName.getOrElse(dataSourceName, Map())
      val format = dataSourceConfig.getOrElse(FORMAT, step.`type`)
      val stepWithDataSourceConfig = step.copy(options = dataSourceConfig ++ step.options)

      if (applicationType.equalsIgnoreCase(ADVANCED_APPLICATION) || applicationType.equalsIgnoreCase(TRIAL_APPLICATION)) {
        if (flagsConfig.enableRecordTracking) {
          new RecordTrackingProcessor(foldersConfig.recordTrackingFolderPath).trackRecords(df._2, dataSourceName, format, stepWithDataSourceConfig)
        }
        //also track records if sink is used in validations, should be saved in separate folder to make sure it is not confused with record tracking/deleting
        //this whole folder can be deleted when validations are finished
        if (dataSourcesUsedInValidation.contains(dataSourceName)) {
          new RecordTrackingProcessor(foldersConfig.recordTrackingForValidationFolderPath).trackRecords(df._2, dataSourceName, format, stepWithDataSourceConfig)
        }
      } else if (applicationType.equalsIgnoreCase(BASIC_APPLICATION) && flagsConfig.enableRecordTracking) {
        LOGGER.warn(s"Please upgrade from the free plan to paid plan to enable record tracking. More details here: $DATA_CATERER_SITE_PRICING")
      }

      val sinkResult = sinkFactory.pushToSink(df._2, dataSourceName, stepWithDataSourceConfig, flagsConfig, startTime)
      DataSourceResult(dataSourceName, task, stepWithDataSourceConfig, sinkResult, batchNum)
    })
  }

  private def getDataSourcesUsedInValidation(optValidations: Option[List[ValidationConfiguration]]): List[String] = {
    optValidations.map(validations => validations.flatMap(vc => {
      val baseDataSourcesWithValidation = vc.dataSources.keys.toList
      val dataSourcesUsedAsUpstreamValidation = vc.dataSources.flatMap(_._2.flatMap(_.validations.map(_.validation)))
        .map {
          case UpstreamDataSourceValidation(_, upstreamDataSource, _, _, _) => Some(upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName)
          case _ => None
        }.filter(_.isDefined)
        .map(_.get)
      baseDataSourcesWithValidation ++ dataSourcesUsedAsUpstreamValidation
    })).getOrElse(List())
  }

  private def getDataFaker(plan: Plan): Faker with Serializable = {
    val optSeed = plan.sinkOptions.flatMap(_.seed)
    val optLocale = plan.sinkOptions.flatMap(_.locale)
    val trySeed = Try(optSeed.map(_.toInt).get)

    (optSeed, trySeed, optLocale) match {
      case (None, _, Some(locale)) =>
        LOGGER.info(s"Locale defined at plan level. All data will be generated with the set locale, locale=$locale")
        new Faker(Locale.forLanguageTag(locale)) with Serializable
      case (Some(_), Success(seed), Some(locale)) =>
        LOGGER.info(s"Seed and locale defined at plan level. All data will be generated with the set seed and locale, seed-value=$seed, locale=$locale")
        new Faker(Locale.forLanguageTag(locale), new Random(seed)) with Serializable
      case (Some(_), Failure(exception), _) =>
        throw new RuntimeException("Failed to get seed value from plan sink options", exception)
      case _ => new Faker() with Serializable
    }
  }
}

