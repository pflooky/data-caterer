package com.github.pflooky.datagen.core.model

import java.time.{Duration, LocalDateTime}

case class DataSourceResultSummary(name: String, numRecords: Long, isSuccess: Boolean, dataSourceResults: List[DataSourceResult])
case class DataSourceResult(name: String, task: Task, step: Step, sinkResult: SinkResult, batchNum: Int = 0)

case class PlanResult(plan: Plan, taskResults: List[TaskResultSummary])

case class TaskResultSummary(task: Task, numRecords: Long, isSuccess: Boolean, stepResults: List[StepResultSummary])
case class TaskResult(task: Task, stepResults: List[StepResult])

case class StepResultSummary(step: Step, numRecords: Long, isSuccess: Boolean, dataSourceResults: List[DataSourceResult])
case class StepResult(step: Step, options: Map[String, String], count: Long, isSuccess: Boolean, sinkResults: List[SinkResult])

case class SinkResult(name: String, format: String, saveMode: String, options: Map[String, String] = Map(), count: Long = -1,
                      isSuccess: Boolean = true, sample: Array[String] = Array(), startTime: LocalDateTime = LocalDateTime.now(),
                      endTime: LocalDateTime = LocalDateTime.now(), generatedMetadata: Array[Field] = Array()) {

  def durationInSeconds: Long = Duration.between(startTime, endTime).toSeconds
}
