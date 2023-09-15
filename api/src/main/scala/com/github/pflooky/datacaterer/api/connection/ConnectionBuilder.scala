package com.github.pflooky.datacaterer.api.connection

import com.github.pflooky.datacaterer.api.model.Constants.FORMAT
import com.github.pflooky.datacaterer.api.model.{Step, Task}
import com.github.pflooky.datacaterer.api.{ConnectionConfigWithTaskBuilder, CountBuilder, FieldBuilder, GeneratorBuilder, SchemaBuilder, StepBuilder, TaskBuilder, TasksBuilder, ValidationBuilder}

import scala.annotation.varargs

trait ConnectionTaskBuilder[T] {
  var connectionConfigWithTaskBuilder: ConnectionConfigWithTaskBuilder = ConnectionConfigWithTaskBuilder()
  var task: Option[TaskBuilder] = None
  var step: Option[StepBuilder] = None

  def apply(builder: ConnectionConfigWithTaskBuilder, optTask: Option[Task], optStep: Option[Step]) = {
    this.connectionConfigWithTaskBuilder = builder
    this.task = optTask.map(TaskBuilder)
    this.step = optStep.map(s => StepBuilder(s))
  }

  def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[T]): T

  @varargs def schema(fields: FieldBuilder*): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.schema(fields: _*))
    this
  }

  def schema(schemaBuilder: SchemaBuilder): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.schema(schemaBuilder))
    this
  }

  def count(countBuilder: CountBuilder): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.count(countBuilder))
    this
  }

  def count(generatorBuilder: GeneratorBuilder): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.count(generatorBuilder))
    this
  }

  def numPartitions(numPartitions: Int): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.numPartitions(numPartitions))
    this
  }

  @varargs def validations(validationBuilders: ValidationBuilder*): ConnectionTaskBuilder[T] = {
    this.step = Some(getStep.validations(validationBuilders: _*))
    this
  }

  def task(taskBuilder: TaskBuilder): ConnectionTaskBuilder[T] = {
    this.task = Some(taskBuilder)
    this
  }

  def task(task: Task): ConnectionTaskBuilder[T] = {
    this.task = Some(TaskBuilder(task))
    this
  }

  def toTasksBuilder: Option[TasksBuilder] = {
    val dataSourceName = connectionConfigWithTaskBuilder.dataSourceName
    val format = connectionConfigWithTaskBuilder.options(FORMAT)
    val optBaseTask = (task, step) match {
      case (Some(task), Some(step)) => Some(task.steps(step.`type`(format)))
      case (Some(task), None) => Some(task)
      case (None, Some(step)) => Some(TaskBuilder().steps(step.`type`(format)))
      case _ => None
    }

    optBaseTask.map(TasksBuilder().addTasks(dataSourceName, _))
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

case class FileBuilder() extends ConnectionTaskBuilder[FileBuilder] {
  override def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[FileBuilder]): FileBuilder = {
    this.connectionConfigWithTaskBuilder = connectionTaskBuilder.connectionConfigWithTaskBuilder
    this
  }

  @varargs def partitionBy(partitionsBy: String*): FileBuilder = {
    this.step = Some(getStep.partitionBy(partitionsBy: _*))
    this
  }
}

trait JdbcBuilder[T] extends ConnectionTaskBuilder[T] {

  def table(table: String): JdbcBuilder[T] = {
    this.step = Some(getStep.jdbcTable(table))
    this
  }

  def table(schema: String, table: String): JdbcBuilder[T] = {
    this.step = Some(getStep.jdbcTable(schema, table))
    this
  }
}

case class PostgresBuilder() extends JdbcBuilder[PostgresBuilder] {
  override def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[PostgresBuilder]): PostgresBuilder = {
    this.connectionConfigWithTaskBuilder = connectionTaskBuilder.connectionConfigWithTaskBuilder
    this
  }
}

case class MySqlBuilder() extends JdbcBuilder[MySqlBuilder] {
  override def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[MySqlBuilder]): MySqlBuilder = {
    this.connectionConfigWithTaskBuilder = connectionTaskBuilder.connectionConfigWithTaskBuilder
    this
  }
}

case class CassandraBuilder() extends ConnectionTaskBuilder[CassandraBuilder] {
  override def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[CassandraBuilder]): CassandraBuilder = {
    this.connectionConfigWithTaskBuilder = connectionTaskBuilder.connectionConfigWithTaskBuilder
    this
  }

  def table(keyspace: String, table: String): CassandraBuilder = {
    this.step = Some(getStep.cassandraTable(keyspace, table))
    this
  }

}

case class SolaceBuilder() extends ConnectionTaskBuilder[SolaceBuilder] {
  override def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[SolaceBuilder]): SolaceBuilder = {
    this.connectionConfigWithTaskBuilder = connectionTaskBuilder.connectionConfigWithTaskBuilder
    this
  }

  def destination(destination: String): SolaceBuilder = {
    this.step = Some(getStep.jmsDestination(destination))
    this
  }

}

case class KafkaBuilder() extends ConnectionTaskBuilder[KafkaBuilder] {
  override def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[KafkaBuilder]): KafkaBuilder = {
    this.connectionConfigWithTaskBuilder = connectionTaskBuilder.connectionConfigWithTaskBuilder
    this
  }

  def topic(topic: String): KafkaBuilder = {
    this.step = Some(getStep.kafkaTopic(topic))
    this
  }

}

case class HttpBuilder() extends ConnectionTaskBuilder[HttpBuilder] {
  override def fromBaseConfig(connectionTaskBuilder: ConnectionTaskBuilder[HttpBuilder]): HttpBuilder = {
    this.connectionConfigWithTaskBuilder = connectionTaskBuilder.connectionConfigWithTaskBuilder
    this
  }
}
