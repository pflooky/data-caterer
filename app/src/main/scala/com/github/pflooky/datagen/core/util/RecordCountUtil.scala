package com.github.pflooky.datagen.core.util

import com.github.pflooky.datagen.core.model.Task

object RecordCountUtil {

  def stepToRecordCountMap(tasks: List[Task], numBatches: Long): Map[String, StepRecordCount] = {
    tasks.flatMap(task =>
      task.steps
        .map(step => {
          val stepRecords = step.count.numRecords
          val averagePerCol = step.count.perColumn.map(perCol => perCol.averageCountPerColumn).getOrElse(1L)
          (
            s"${task.name}_${step.name}",
            StepRecordCount(0L, (stepRecords / averagePerCol) / numBatches, stepRecords)
          )
        })).toMap
  }
}

case class StepRecordCount(currentNumRecords: Long, numRecordsPerBatch: Long, numTotalRecords: Long)
