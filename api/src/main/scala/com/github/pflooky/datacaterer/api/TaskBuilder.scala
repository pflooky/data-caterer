package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.model.Constants._
import com.github.pflooky.datacaterer.api.model.{Count, DataType, Field, Generator, PerColumnCount, Schema, Step, StringType, Task, TaskSummary}
import com.softwaremill.quicklens.ModifyPimp

case class TaskSummaryBuilder(
                               taskSummary: TaskSummary = TaskSummary(DEFAULT_TASK_NAME, "myDefaultDataSource"),
                               task: Option[Task] = None
                             ) {

  def name(name: String): TaskSummaryBuilder = {
    if (task.isEmpty) this.modify(_.taskSummary.name).setTo(name) else this
  }

  def task(taskBuilder: TaskBuilder): TaskSummaryBuilder = {
    this.modify(_.taskSummary.name).setTo(taskBuilder.task.name)
      .modify(_.task).setTo(Some(taskBuilder.task))
  }

  def task(task: Task): TaskSummaryBuilder = {
    this.modify(_.taskSummary.name).setTo(task.name)
      .modify(_.task).setTo(Some(task))
  }

  def dataSource(name: String): TaskSummaryBuilder =
    this.modify(_.taskSummary.dataSourceName).setTo(name)

  def enabled(enabled: Boolean): TaskSummaryBuilder =
    this.modify(_.taskSummary.enabled).setTo(enabled)

}

case class TasksBuilder(tasks: List[Task] = List(), dataSourceName: String = DEFAULT_DATA_SOURCE_NAME) {

  def addTasksWithBuilders(dataSourceName: String, taskBuilder: TaskBuilder*): TasksBuilder =
    this.modify(_.tasks)(_ ++ taskBuilder.map(_.task))
      .modify(_.dataSourceName).setTo(dataSourceName)

  def addTasks(dataSourceName: String, tasks: Task*): TasksBuilder =
    this.modify(_.tasks)(_ ++ tasks)
      .modify(_.dataSourceName).setTo(dataSourceName)

  def addTask(name: String, dataSourceName: String, stepBuilders: StepBuilder*): TasksBuilder =
    this.modify(_.tasks)(_ ++ List(TaskBuilder(Task(name, stepBuilders.map(_.step).toList)).task))
      .modify(_.dataSourceName).setTo(dataSourceName)

  def addTask(name: String, dataSourceName: String, steps: List[Step]): TasksBuilder =
    this.modify(_.tasks)(_ ++ List(TaskBuilder(Task(name, steps)).task))
      .modify(_.dataSourceName).setTo(dataSourceName)
}

case class TaskBuilder(task: Task = Task()) {

  def name(name: String): TaskBuilder = this.modify(_.task.name).setTo(name)

  def step(step: StepBuilder): TaskBuilder = this.modify(_.task.steps)(_ ++ List(step.step))

  def step(step: Step): TaskBuilder = this.modify(_.task.steps)(_ ++ List(step))

  def stepsWithBuilders(stepBuilders: StepBuilder*): TaskBuilder = this.modify(_.task.steps)(_ ++ stepBuilders.map(_.step))

  def steps(steps: Step*): TaskBuilder = this.modify(_.task.steps)(_ ++ steps)
}

case class StepBuilder(step: Step = Step()) {

  def name(name: String): StepBuilder =
    this.modify(_.step.name).setTo(name)

  def `type`(`type`: String): StepBuilder =
    this.modify(_.step.`type`).setTo(`type`)

  def enabled(enabled: Boolean): StepBuilder =
    this.modify(_.step.enabled).setTo(enabled)

  def option(option: (String, String)): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(option))

  def options(options: Map[String, String]): StepBuilder =
    this.modify(_.step.options)(_ ++ options)

  def jdbcTable(table: String): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(JDBC_TABLE -> table))

  def jdbcTable(schema: String, table: String): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(JDBC_TABLE -> s"$schema.$table"))

  def cassandraTable(keyspace: String, table: String): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(CASSANDRA_KEYSPACE -> keyspace, CASSANDRA_TABLE -> table))

  def jmsDestination(destination: String): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(JMS_DESTINATION_NAME -> destination))

  def kafkaTopic(topic: String): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(KAFKA_TOPIC -> topic))

  def path(path: String): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(PATH -> path))

  def partitionBy(partitionBy: String*): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(PARTITION_BY -> partitionBy.map(_.trim).mkString(",")))

  def numPartitions(partitions: Int): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(PARTITIONS -> partitions.toString))

  def rowsPerSecond(rowsPerSecond: Int): StepBuilder =
    this.modify(_.step.options)(_ ++ Map(ROWS_PER_SECOND -> rowsPerSecond.toString))

  def count(countBuilder: CountBuilder): StepBuilder =
    this.modify(_.step.count).setTo(countBuilder.count)

  def count(count: Count): StepBuilder =
    this.modify(_.step.count).setTo(count)

  def count(total: Long): StepBuilder =
    this.modify(_.step.count).setTo(CountBuilder().total(total).count)

  def count(generator: GeneratorBuilder): StepBuilder =
    this.modify(_.step.count).setTo(CountBuilder().generator(generator).count)

  def count(generator: Generator): StepBuilder =
    this.modify(_.step.count).setTo(CountBuilder().generator(generator).count)

  def count(perColumnCountBuilder: PerColumnCountBuilder): StepBuilder =
    this.modify(_.step.count).setTo(CountBuilder().perColumn(perColumnCountBuilder).count)

  def count(perColumnCount: PerColumnCount): StepBuilder =
    this.modify(_.step.count).setTo(CountBuilder().perColumn(perColumnCount).count)

  def schema(schemaBuilder: SchemaBuilder): StepBuilder =
    this.modify(_.step.schema).setTo(schemaBuilder.schema)

  def schema(schema: Schema): StepBuilder =
    this.modify(_.step.schema).setTo(schema)
}

case class CountBuilder(count: Count = Count()) {
  def total(total: Long): CountBuilder =
    this.modify(_.count.total).setTo(Some(total))

  def generator(generator: GeneratorBuilder): CountBuilder =
    this.modify(_.count.generator).setTo(Some(generator.generator))

  def generator(generator: Generator): CountBuilder =
    this.modify(_.count.generator).setTo(Some(generator))

  def perColumn(perColumnCountBuilder: PerColumnCountBuilder): CountBuilder =
    this.modify(_.count.perColumn).setTo(Some(perColumnCountBuilder.perColumnCount))

  def perColumn(perColumnCount: PerColumnCount): CountBuilder =
    this.modify(_.count.perColumn).setTo(Some(perColumnCount))

  def columns(col: String, cols: String*): CountBuilder =
    this.modify(_.count.perColumn).setTo(Some(perColCount.columns(col, cols: _*).perColumnCount))

  def perColumnTotal(total: Long, col: String, cols: String*): CountBuilder =
    this.modify(_.count.perColumn).setTo(Some(perColCount.total(total, col, cols: _*).perColumnCount))

  def perColumnGenerator(generator: GeneratorBuilder, col: String, cols: String*): CountBuilder =
    this.modify(_.count.perColumn).setTo(Some(perColCount.generator(generator, col, cols: _*).perColumnCount))

  def perColumnGenerator(generator: Generator, col: String, cols: String*): CountBuilder =
    this.modify(_.count.perColumn).setTo(Some(perColCount.generator(generator, col, cols: _*).perColumnCount))

  def perColumnGenerator(total: Long, generator: Generator, col: String, cols: String*): CountBuilder =
    this.modify(_.count.perColumn).setTo(Some(perColCount.generator(total, generator, col, cols: _*).perColumnCount))

  private def perColCount: PerColumnCountBuilder = {
    count.perColumn match {
      case Some(value) => PerColumnCountBuilder(value)
      case None => PerColumnCountBuilder()
    }
  }
}

case class PerColumnCountBuilder(perColumnCount: PerColumnCount = PerColumnCount()) {
  def columns(col: String, cols: String*): PerColumnCountBuilder =
    this.modify(_.perColumnCount.columnNames).setTo((col +: cols).toList)

  def total(total: Long, col: String, cols: String*): PerColumnCountBuilder =
    columns(col, cols: _*).modify(_.perColumnCount.count).setTo(Some(total))

  def generator(generator: GeneratorBuilder, col: String, cols: String*): PerColumnCountBuilder =
    columns(col, cols: _*).modify(_.perColumnCount.generator).setTo(Some(generator.generator))

  def generator(generator: Generator, col: String, cols: String*): PerColumnCountBuilder =
    columns(col, cols: _*).modify(_.perColumnCount.generator).setTo(Some(generator))

  def generator(total: Long, generator: Generator, col: String, cols: String*): PerColumnCountBuilder =
    this.total(total, col, cols: _*).modify(_.perColumnCount.generator).setTo(Some(generator))
}

case class SchemaBuilder(schema: Schema = Schema()) {
  def addField(name: String, `type`: DataType = StringType): SchemaBuilder =
    addFields(FieldBuilder().name(name).`type`(`type`))

  def addField(field: FieldBuilder): SchemaBuilder =
    addFields(field)

  def addFieldsJava(fields: Field*): SchemaBuilder =
    this.modify(_.schema.fields).setTo(schema.fields match {
      case Some(value) => Some(value ++ fields)
      case None => Some(fields.toList)
    })

  def addFields(fields: FieldBuilder*): SchemaBuilder =
    this.modify(_.schema.fields).setTo(schema.fields match {
      case Some(value) => Some(value ++ fields.map(_.field))
      case None => Some(fields.map(_.field).toList)
    })
}

case class FieldBuilder(field: Field = Field()) {
  def name(name: String): FieldBuilder =
    this.modify(_.field.name).setTo(name)

  def `type`(`type`: DataType): FieldBuilder =
    this.modify(_.field.`type`).setTo(Some(`type`.toString))

  def schema(schema: SchemaBuilder): FieldBuilder =
    this.modify(_.field.schema).setTo(Some(schema.schema))

  def schema(schema: Schema): FieldBuilder =
    this.modify(_.field.schema).setTo(Some(schema))

  def nullable(nullable: Boolean): FieldBuilder =
    this.modify(_.field.nullable).setTo(nullable)

  def generator(generator: GeneratorBuilder): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(generator.generator))

  def generator(generator: Generator): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(generator))

  def random: FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.random.generator))

  def sql(sql: String): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.sql(sql).generator))

  def regex(regex: String): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.regex(regex).generator))

  def oneOf(values: Any*): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.oneOf(values: _*).generator))
      .modify(_.field.`type`)
      .setTo(
        values match {
          case Seq(_: Double, _*) => Some("double")
          case Seq(_: String, _*) => Some("string")
          case Seq(_: Int, _*) => Some("integer")
          case Seq(_: Long, _*) => Some("long")
          case Seq(_: Boolean, _*) => Some("boolean")
          case _ => None
        }
      )

  def options(options: Map[String, Any]): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.options(options).generator))

  def option(option: (String, Any)): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.option(option).generator))

  def seed(seed: Long): FieldBuilder = this.modify(_.field.generator).setTo(Some(getGenBuilder.seed(seed).generator))

  def enableNull(enable: Boolean): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.enableNull(enable).generator))

  def nullProbability(probability: Double): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.nullProbability(probability).generator))

  def enableEdgeCases(enable: Boolean): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.enableEdgeCases(enable).generator))

  def edgeCaseProbability(probability: Double): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.edgeCaseProbability(probability).generator))

  def static(value: Any): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.static(value).generator))

  def staticValue(value: Any): FieldBuilder = static(value)

  def unique(isUnique: Boolean): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.unique(isUnique).generator))

  def arrayType(`type`: String): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.arrayType(`type`).generator))

  def expression(expr: String): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.expression(expr).generator))

  def avgLength(length: Int): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.avgLength(length).generator))

  def min(min: Any): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.min(min).generator))

  def minLength(length: Int): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.minLength(length).generator))

  def listMinLength(length: Int): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.listMinLength(length).generator))

  def max(max: Any): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.max(max).generator))

  def maxLength(length: Int): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.maxLength(length).generator))

  def listMaxLength(length: Int): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.listMaxLength(length).generator))

  def numericPrecision(precision: Int): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.numericPrecision(precision).generator))

  def numericScale(scale: Int): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.numericScale(scale).generator))

  def omit(omit: Boolean): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.omit(omit).generator))

  def primaryKey(isPrimaryKey: Boolean): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.primaryKey(isPrimaryKey).generator))

  def primaryKeyPosition(position: Int): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.primaryKeyPosition(position).generator))

  def clusteringPosition(position: Int): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.clusteringPosition(position).generator))

  private def getGenBuilder: GeneratorBuilder = {
    field.generator match {
      case Some(gen) => GeneratorBuilder(gen)
      case None => GeneratorBuilder()
    }
  }
}

case class GeneratorBuilder(generator: Generator = Generator()) {
  def random: GeneratorBuilder =
    this.modify(_.generator.`type`).setTo(RANDOM_GENERATOR)

  def sql(sql: String): GeneratorBuilder =
    this.modify(_.generator.`type`).setTo(SQL_GENERATOR)
      .modify(_.generator.options)(_ ++ Map(SQL_GENERATOR -> sql))

  def regex(regex: String): GeneratorBuilder =
    this.modify(_.generator.`type`).setTo(REGEX_GENERATOR)
      .modify(_.generator.options)(_ ++ Map(REGEX_GENERATOR -> regex))

  def oneOf(values: Any*): GeneratorBuilder = {
    this.modify(_.generator.`type`).setTo(ONE_OF_GENERATOR)
      .modify(_.generator.options)(_ ++ Map(ONE_OF_GENERATOR -> values))
  }

  def options(options: Map[String, Any]): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ options)

  def option(option: (String, Any)): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(option))

  def seed(seed: Long): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(RANDOM_SEED -> seed.toString))

  def enableNull(enable: Boolean): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(ENABLED_NULL -> enable.toString))

  def nullProbability(probability: Double): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(PROBABILITY_OF_NULL -> probability.toString))

  def enableEdgeCases(enable: Boolean): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(ENABLED_EDGE_CASE -> enable.toString))

  def edgeCaseProbability(probability: Double): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(PROBABILITY_OF_EDGE_CASE -> probability.toString))

  def static(value: Any): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(STATIC -> value.toString))

  def staticValue(value: Any): GeneratorBuilder = static(value)

  def unique(isUnique: Boolean): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(IS_UNIQUE -> isUnique.toString))

  def arrayType(`type`: String): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(ARRAY_TYPE -> `type`))

  def expression(expr: String): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(EXPRESSION -> expr))

  def avgLength(length: Int): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(AVERAGE_LENGTH -> length.toString))

  def min(min: Any): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(MINIMUM -> min.toString))

  def minLength(length: Int): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(MINIMUM_LENGTH -> length.toString))

  def listMinLength(length: Int): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(LIST_MINIMUM_LENGTH -> length.toString))

  def max(max: Any): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(MAXIMUM -> max.toString))

  def maxLength(length: Int): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(MAXIMUM_LENGTH -> length.toString))

  def listMaxLength(length: Int): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(LIST_MAXIMUM_LENGTH -> length.toString))

  def numericPrecision(precision: Int): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(NUMERIC_PRECISION -> precision.toString))

  def numericScale(scale: Int): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(NUMERIC_SCALE -> scale.toString))

  def omit(omit: Boolean): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(OMIT -> omit.toString))

  def primaryKey(isPrimaryKey: Boolean): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(IS_PRIMARY_KEY -> isPrimaryKey.toString))

  def primaryKeyPosition(position: Int): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(PRIMARY_KEY_POSITION -> position.toString))

  def clusteringPosition(position: Int): GeneratorBuilder =
    this.modify(_.generator.options)(_ ++ Map(CLUSTERING_POSITION -> position.toString))
}