package com.github.pflooky.datacaterer.api.connection

import com.github.pflooky.datacaterer.api.model.Constants.FORMAT
import com.github.pflooky.datacaterer.api.model.{Count, Field, Schema, Step, Task}
import com.github.pflooky.datacaterer.api.{ConnectionConfigWithTaskBuilder, CountBuilder, FieldBuilder, GeneratorBuilder, SchemaBuilder, StepBuilder, TaskBuilder, TasksBuilder}

abstract class ConnectionTaskBuilder {
  var connectionConfigWithTaskBuilder: ConnectionConfigWithTaskBuilder = ConnectionConfigWithTaskBuilder()
  var task: Option[TaskBuilder] = None
  var step: Option[StepBuilder] = None

  def apply(builder: ConnectionConfigWithTaskBuilder, optTask: Option[Task], optStep: Option[Step]) = {
    this.connectionConfigWithTaskBuilder = builder
    this.task = optTask.map(TaskBuilder)
    this.step = optStep.map(StepBuilder)
  }

  def schema(field: FieldBuilder, fields: FieldBuilder*): ConnectionTaskBuilder = {
    this.step = Some(getStep.schema(field, fields: _*))
    this
  }

  def schema(field: Field, fields: Field*): ConnectionTaskBuilder = {
    this.step = Some(getStep.schema(field, fields: _*))
    this
  }

  def schema(schemaBuilder: SchemaBuilder): ConnectionTaskBuilder = {
    this.step = Some(getStep.schema(schemaBuilder))
    this
  }

  def schema(schema: Schema): ConnectionTaskBuilder = {
    this.step = Some(getStep.schema(schema))
    this
  }

  def count(countBuilder: CountBuilder): ConnectionTaskBuilder = {
    this.step = Some(getStep.count(countBuilder))
    this
  }

  def count(generatorBuilder: GeneratorBuilder): ConnectionTaskBuilder = {
    this.step = Some(getStep.count(generatorBuilder))
    this
  }

  def count(count: Count): ConnectionTaskBuilder = {
    this.step = Some(getStep.count(count))
    this
  }

  def numPartitions(numPartitions: Int): ConnectionTaskBuilder = {
    this.step = Some(getStep.numPartitions(numPartitions))
    this
  }

  def task(taskBuilder: TaskBuilder): ConnectionTaskBuilder = {
    this.task = Some(taskBuilder)
    this
  }

  def task(task: Task): ConnectionTaskBuilder = {
    this.task = Some(TaskBuilder(task))
    this
  }

  def toTasksBuilder: TasksBuilder = {
    val dataSourceName = connectionConfigWithTaskBuilder.dataSourceName
    val format = connectionConfigWithTaskBuilder.options(FORMAT)
    val baseTask = (task, step) match {
      case (Some(task), Some(step)) => task.steps(step.`type`(format))
      case (Some(task), None) => task
      case (None, Some(step)) => TaskBuilder().steps(step.`type`(format))
      case _ =>
        throw new RuntimeException(s"Need to define at least 1 task or step to execute for connection, data-source-name=$dataSourceName")
    }

    TasksBuilder().addTasks(dataSourceName, baseTask)
  }

  protected def getStep: StepBuilder = step match {
    case Some(value) => value
    case None => StepBuilder()
  }

  protected def getTask: TaskBuilder = task match {
    case Some(value) => value
    case None => TaskBuilder()
  }
}

case class FileBuilder() extends ConnectionTaskBuilder {

  def partitionBy(partitionBy: String, partitionsBy: String*): FileBuilder = {
    this.step = Some(getStep.partitionBy(partitionBy, partitionsBy: _*))
    this
  }
}

case class JdbcBuilder() extends ConnectionTaskBuilder {

  def table(table: String): JdbcBuilder = {
    this.step = Some(getStep.jdbcTable(table))
    this
  }

  def table(schema: String, table: String): JdbcBuilder = {
    this.step = Some(getStep.jdbcTable(schema, table))
    this
  }

}

case class CassandraBuilder() extends ConnectionTaskBuilder {

  def table(keyspace: String, table: String): CassandraBuilder = {
    this.step = Some(getStep.cassandraTable(keyspace, table))
    this
  }

}

case class SolaceBuilder() extends ConnectionTaskBuilder {

  def destination(destination: String): SolaceBuilder = {
    this.step = Some(getStep.jmsDestination(destination))
    this
  }

}

case class KafkaBuilder() extends ConnectionTaskBuilder {

  def topic(topic: String): KafkaBuilder = {
    this.step = Some(getStep.kafkaTopic(topic))
    this
  }

}

case class HttpBuilder() extends ConnectionTaskBuilder
