package com.github.pflooky.datagen.core.generator

import com.github.pflooky.datacaterer.api.model.Constants.FORMAT
import com.github.pflooky.datacaterer.api.model.{FlagsConfig, FoldersConfig, GenerationConfig, MetadataConfig, Plan, Task, TaskSummary}
import com.github.pflooky.datagen.core.generator.track.RecordTrackingProcessor
import com.github.pflooky.datagen.core.model.Constants.{ADVANCED_APPLICATION, BASIC_APPLICATION, DATA_CATERER_SITE_PRICING}
import com.github.pflooky.datagen.core.model.DataSourceResult
import com.github.pflooky.datagen.core.model.PlanImplicits.StepOps
import com.github.pflooky.datagen.core.sink.SinkFactory
import com.github.pflooky.datagen.core.util.GeneratorUtil.getDataSourceName
import com.github.pflooky.datagen.core.util.RecordCountUtil.calculateNumBatches
import com.github.pflooky.datagen.core.util.{ForeignKeyUtil, UniqueFieldsUtil}
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class BatchDataProcessor(connectionConfigsByName: Map[String, Map[String, String]], foldersConfig: FoldersConfig,
                         metadataConfig: MetadataConfig, flagsConfig: FlagsConfig, generationConfig: GenerationConfig, applicationType: String)(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private lazy val recordTrackingProcessor = new RecordTrackingProcessor(foldersConfig.recordTrackingFolderPath)
  private lazy val sinkFactory = new SinkFactory(connectionConfigsByName, flagsConfig, metadataConfig, applicationType)

  def splitAndProcess(plan: Plan, executableTasks: List[(TaskSummary, Task)], faker: Faker)
                     (implicit sparkSession: SparkSession): List[DataSourceResult] = {
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
          genDf.select()
          val dfRecordCount = if (flagsConfig.enableCount) genDf.count() else stepRecords.numRecordsPerBatch
          LOGGER.debug(s"Step record count for batch, batch=$batch, step-name=${s.name}, target-num-records=${stepRecords.numRecordsPerBatch}, actual-num-records=$dfRecordCount")
          trackRecordsPerStep = trackRecordsPerStep ++ Map(recordStepName -> stepRecords.copy(currentNumRecords = dfRecordCount))

          val df = if (s.gatherPrimaryKeys.nonEmpty && flagsConfig.enableUniqueCheck) uniqueFieldUtil.getUniqueFieldsValues(dataSourceStepName, genDf) else genDf
          if (!df.storageLevel.useMemory) df.cache()
          (dataSourceStepName, df)
        })
      ).toMap

      val sinkDf = plan.sinkOptions
        .map(_ => ForeignKeyUtil.getDataFramesWithForeignKeys(plan, generatedDataForeachTask))
        .getOrElse(generatedDataForeachTask.toList)
      val sinkResults = pushDataToSinks(executableTasks, sinkDf, batch, startTime)
      sinkDf.foreach(_._2.unpersist())
      LOGGER.info(s"Finished batch, batch=$batch, num-batches=$numBatches")
      sinkResults
    }).toList
    LOGGER.debug(s"Completed all batches, num-batches=$numBatches")
    dataSourceResults
  }

  def pushDataToSinks(executableTasks: List[(TaskSummary, Task)], sinkDf: List[(String, DataFrame)], batchNum: Int, startTime: LocalDateTime): List[DataSourceResult] = {
    val stepAndTaskByDataSourceName = executableTasks.flatMap(task =>
      task._2.steps.map(s => (getDataSourceName(task._1, s), (s, task._2)))
    ).toMap

    sinkDf.map(df => {
      val dataSourceName = df._1.split("\\.").head
      val (step, task) = stepAndTaskByDataSourceName(df._1)
      val sinkResult = sinkFactory.pushToSink(df._2, dataSourceName, step, flagsConfig, startTime)

      if (applicationType.equalsIgnoreCase(ADVANCED_APPLICATION) && flagsConfig.enableRecordTracking) {
        val format = connectionConfigsByName.get(dataSourceName).map(_(FORMAT)).getOrElse(step.`type`)
        recordTrackingProcessor.trackRecords(df._2, dataSourceName, format, step)
      } else if (applicationType.equalsIgnoreCase(BASIC_APPLICATION) && flagsConfig.enableRecordTracking) {
        LOGGER.warn(s"Please upgrade from the free plan to paid plan to enable record tracking. More details here: $DATA_CATERER_SITE_PRICING")
      }
      DataSourceResult(dataSourceName, task, step, sinkResult, batchNum)
    })
  }
}

