package com.github.pflooky.datagen.core.listener

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}

import scala.collection.mutable.ListBuffer

class SparkRecordListener(enableCount: Boolean = true) extends SparkListener {

  var outputRows: ListBuffer[SparkTaskRecordSummary] = ListBuffer()

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    if (taskEnd.taskType.equalsIgnoreCase("resulttask") && enableCount) {
      synchronized {
        outputRows.append(SparkTaskRecordSummary(taskEnd.taskInfo.finishTime, taskEnd.taskMetrics.outputMetrics.recordsWritten))
      }
    }
  }
}

case class SparkTaskRecordSummary(finishTime: Long, numRecords: Long)
