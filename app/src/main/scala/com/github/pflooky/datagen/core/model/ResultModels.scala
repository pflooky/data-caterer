package com.github.pflooky.datagen.core.model

import com.github.pflooky.datacaterer.api.model.{Field, Step, Task}

import java.time.{Duration, LocalDateTime}

case class DataSourceResultSummary(
                                    name: String,
                                    numRecords: Long,
                                    isSuccess: Boolean,
                                    dataSourceResults: List[DataSourceResult]
                                  )

case class DataSourceResult(
                             name: String,
                             task: Task,
                             step: Step,
                             sinkResult: SinkResult,
                             batchNum: Int = 0
                           )

case class TaskResultSummary(
                              task: Task,
                              numRecords: Long,
                              isSuccess: Boolean,
                              stepResults: List[StepResultSummary]
                            )

case class StepResultSummary(
                              step: Step,
                              numRecords: Long,
                              isSuccess: Boolean,
                              dataSourceResults: List[DataSourceResult]
                            )

case class SinkResult(
                       name: String,
                       format: String,
                       saveMode: String,
                       options: Map[String, String] = Map(),
                       count: Long = -1,
                       isSuccess: Boolean = true,
                       sample: Array[String] = Array(),
                       startTime: LocalDateTime = LocalDateTime.now(),
                       endTime: LocalDateTime = LocalDateTime.now(),
                       generatedMetadata: Array[Field] = Array(),
                       exception: Option[Throwable] = None
                     ) {

  def durationInSeconds: Long = Duration.between(startTime, endTime).toSeconds
}
