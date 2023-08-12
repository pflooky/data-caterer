package com.github.pflooky.datagen.core.generator

import com.github.pflooky.datagen.core.config.GenerationConfig
import com.github.pflooky.datagen.core.generator.track.RecordTrackingProcessor
import com.github.pflooky.datagen.core.model.Constants.{ADVANCED_APPLICATION, BASIC_APPLICATION, DATA_CATERER_SITE_PRICING, FORMAT}
import com.github.pflooky.datagen.core.model.{Plan, Task, TaskSummary}
import com.github.pflooky.datagen.core.sink.SinkFactory
import com.github.pflooky.datagen.core.util.GeneratorUtil.getDataSourceName
import com.github.pflooky.datagen.core.util.{ForeignKeyUtil, RecordCountUtil, SparkProvider, UniqueFieldsUtil}
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

class BatchDataProcessor extends SparkProvider {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private lazy val recordTrackingProcessor = new RecordTrackingProcessor(foldersConfig.recordTrackingFolderPath)
  private lazy val sinkFactory = new SinkFactory(connectionConfigsByName, applicationType)

  def splitAndProcess(plan: Plan, executableTasks: List[(TaskSummary, Task)], faker: Faker)
                     (implicit sparkSession: SparkSession): Unit = {
    val dataGeneratorFactory = new DataGeneratorFactory(faker)
    val uniqueFieldUtil = new UniqueFieldsUtil(executableTasks)
    val tasks = executableTasks.map(_._2)

    val countPerStep = getCountPerStep(tasks, generationConfig).toMap
    val totalRecordsToGenerate = countPerStep.values.sum
    if (totalRecordsToGenerate <= generationConfig.numRecordsPerBatch) {
      LOGGER.debug(s"Generating all records for all steps in single batch, total-records=$totalRecordsToGenerate, configured-records-per-batch=${generationConfig.numRecordsPerBatch}")
    }

    val numBatches = Math.max((totalRecordsToGenerate / generationConfig.numRecordsPerBatch).toInt, 1)
    LOGGER.debug(s"Number of batches for data generation, num-batches=$numBatches")
    var trackRecordsPerStep = RecordCountUtil.stepToRecordCountMap(tasks, numBatches)

    (1 to numBatches).foreach(batch => {
      LOGGER.info(s"Starting batch, batch=$batch, num-batches=$numBatches")
      val generatedDataForeachTask = executableTasks.flatMap(task =>
        task._2.steps.map(s => {
          val dataSourceStepName = getDataSourceName(task._1, s)
          val recordStepName = s"${task._2.name}_${s.name}"
          val stepRecords = trackRecordsPerStep(recordStepName)
          val startIndex = stepRecords.currentNumRecords
          val endIndex = stepRecords.currentNumRecords + stepRecords.numRecordsPerBatch

          val genDf = dataGeneratorFactory.generateDataForStep(s, task._1.dataSourceName, startIndex, endIndex)
          val dfRecordCount = if (flagsConfig.enableCount) genDf.count() else stepRecords.numRecordsPerBatch
          LOGGER.debug(s"Step record count for batch, batch=$batch, step-name=${s.name}, target-num-records=${stepRecords.numRecordsPerBatch}, actual-num-records=$dfRecordCount")
          trackRecordsPerStep = trackRecordsPerStep ++ Map(recordStepName -> stepRecords.copy(currentNumRecords = dfRecordCount))

          val df = if (s.gatherPrimaryKeys.nonEmpty && generationConfig.enableUniqueCheck) uniqueFieldUtil.getUniqueFieldsValues(dataSourceStepName, genDf) else genDf
          if (!df.storageLevel.useMemory) df.cache()
          (dataSourceStepName, df)
        })
      ).toMap

      val sinkDf = plan.sinkOptions
        .map(_ => ForeignKeyUtil.getDataFramesWithForeignKeys(plan, generatedDataForeachTask))
        .getOrElse(generatedDataForeachTask.toList)
      pushDataToSinks(executableTasks, sinkDf)
      sinkDf.foreach(_._2.unpersist())
      LOGGER.info(s"Finished batch, batch=$batch, num-batches=$numBatches")
    })
    LOGGER.debug(s"Completed all batches, num-batches=$numBatches")
  }

  def pushDataToSinks(executableTasks: List[(TaskSummary, Task)], sinkDf: List[(String, DataFrame)]): Unit = {
    val stepByDataSourceName = executableTasks.flatMap(task => task._2.steps.map(s => (getDataSourceName(task._1, s), s))).toMap

    sinkDf.foreach(df => {
      val dataSourceName = df._1.split("\\.").head
      val step = stepByDataSourceName(df._1)
      sinkFactory.pushToSink(df._2, dataSourceName, step, flagsConfig)

      if (applicationType.equalsIgnoreCase(ADVANCED_APPLICATION) && flagsConfig.enableRecordTracking) {
        val format = connectionConfigsByName(dataSourceName)(FORMAT)
        recordTrackingProcessor.trackRecords(df._2, dataSourceName, format, step)
      } else if (applicationType.equalsIgnoreCase(BASIC_APPLICATION) && flagsConfig.enableRecordTracking) {
        LOGGER.warn(s"Please upgrade from the free plan to paid plan to enable record tracking. More details here: $DATA_CATERER_SITE_PRICING")
      }
    })
  }

  private def getCountPerStep(tasks: List[Task], generationConfig: GenerationConfig): List[(String, Long)] = {
    //TODO need to take into account the foreign keys defined
    //the main foreign key controls the number of records produced by the children data sources
    val baseStepCounts = tasks.flatMap(task => {
      task.steps.map(step => {
        val stepName = s"${task.name}_${step.name}"
        val stepCount = generationConfig.numRecordsPerStep
          .map(c => {
            LOGGER.debug(s"Step count total is defined in generation config, overriding count total defined in step, " +
              s"task-name=${task.name}, step-name=${step.name}, records-per-step=$c")
            step.count.copy(total = Some(c))
          })
          .getOrElse(step.count)
        (stepName, stepCount.numRecords)
      })
    })
    baseStepCounts
  }
}

