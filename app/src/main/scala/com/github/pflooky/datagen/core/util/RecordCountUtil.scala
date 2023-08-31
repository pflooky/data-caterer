package com.github.pflooky.datagen.core.util

import com.github.pflooky.datacaterer.api.model.{GenerationConfig, Task}
import com.github.pflooky.datagen.core.model.PlanImplicits.{CountOps, PerColumnCountOps}
import org.apache.log4j.Logger

object RecordCountUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def calculateNumBatches(tasks: List[Task], generationConfig: GenerationConfig): (Int, Map[String, StepRecordCount]) = {
    val countPerStep = getCountPerStep(tasks, generationConfig).toMap
    val totalRecordsToGenerate = countPerStep.values.sum
    if (totalRecordsToGenerate <= generationConfig.numRecordsPerBatch) {
      LOGGER.debug(s"Generating all records for all steps in single batch, total-records=$totalRecordsToGenerate, configured-records-per-batch=${generationConfig.numRecordsPerBatch}")
    }

    val numBatches = Math.max(Math.ceil(totalRecordsToGenerate / generationConfig.numRecordsPerBatch.toDouble).toInt, 1)
    LOGGER.info(s"Number of batches for data generation, num-batches=$numBatches, num-records-per-batch=${generationConfig.numRecordsPerBatch}, total-records=$totalRecordsToGenerate")
    val trackRecordsPerStep = stepToRecordCountMap(tasks, numBatches)
    (numBatches, trackRecordsPerStep)
  }

  private def stepToRecordCountMap(tasks: List[Task], numBatches: Long): Map[String, StepRecordCount] = {
    tasks.flatMap(task =>
      task.steps
        .map(step => {
          val stepRecords = step.count.numRecords
          val averagePerCol = step.count.perColumn.map(_.averageCountPerColumn).getOrElse(1L)
          (
            s"${task.name}_${step.name}",
            StepRecordCount(0L, (stepRecords / averagePerCol) / numBatches, stepRecords)
          )
        })).toMap
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

case class StepRecordCount(currentNumRecords: Long, numRecordsPerBatch: Long, numTotalRecords: Long)
