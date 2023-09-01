package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.model.Constants._
import com.github.pflooky.datacaterer.api.model.{Count, Field, Generator, PerColumnCount, Schema, Step, Task, TaskSummary}
import com.softwaremill.quicklens.ModifyPimp

case class TaskSummaryBuilder(taskSummary: TaskSummary = TaskSummary("default task summary", "myDefaultDataSource")) {

  def name(name: String): TaskSummaryBuilder =
    this.modify(_.taskSummary.name).setTo(name)

  def task(taskBuilder: TaskBuilder): TaskSummaryBuilder =
    this.modify(_.taskSummary.name).setTo(taskBuilder.task.name)

  def dataSourceName(dataSourceName: String): TaskSummaryBuilder =
    this.modify(_.taskSummary.dataSourceName).setTo(dataSourceName)

  def enabled(enabled: Boolean): TaskSummaryBuilder =
    this.modify(_.taskSummary.enabled).setTo(enabled)

}

case class TasksBuilder(tasks: List[TaskBuilder] = List()) {
  def addTask(stepBuilder: StepBuilder): TasksBuilder =
    addTask("default_task", "json", stepBuilder)

  def addTask(name: String, dataSourceName: String, stepBuilder: StepBuilder): TasksBuilder =
    addTask(name, dataSourceName, stepBuilder)

  def addTask(name: String, dataSourceName: String, steps: StepBuilder*): TasksBuilder =
    this.modify(_.tasks)(_ ++ List(TaskBuilder(Task(name, steps.map(_.step).toList), dataSourceName)))
}

case class TaskBuilder(task: Task = Task(), dataSourceName: String = "json") {

  def name(name: String): TaskBuilder = this.modify(_.task.name).setTo(name)

  def step(step: StepBuilder): TaskBuilder = this.modify(_.task.steps)(_ ++ List(step.step))

  def steps(steps: StepBuilder*): TaskBuilder = this.modify(_.task.steps)(_ ++ steps.map(_.step))
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

  def count(countBuilder: CountBuilder): StepBuilder =
    this.modify(_.step.count).setTo(countBuilder.count)

  def count(total: Long): StepBuilder =
    this.modify(_.step.count).setTo(CountBuilder().total(total).count)

  def count(generator: GeneratorBuilder): StepBuilder =
    this.modify(_.step.count).setTo(CountBuilder().generator(generator).count)

  def schema(schemaBuilder: SchemaBuilder): StepBuilder =
    this.modify(_.step.schema).setTo(schemaBuilder.schema)
}

case class CountBuilder(count: Count = Count()) {
  def total(total: Long): CountBuilder =
    this.modify(_.count.total).setTo(Some(total))

  def generator(generator: GeneratorBuilder): CountBuilder =
    this.modify(_.count.generator).setTo(Some(generator.generator))

  def perColumn(perColumnCountBuilder: PerColumnCountBuilder): CountBuilder =
    this.modify(_.count.perColumn).setTo(Some(perColumnCountBuilder.perColumnCount))

  def columns(cols: String*): CountBuilder =
    this.modify(_.count.perColumn).setTo(Some(perColCount.columns(cols: _*).perColumnCount))

  def perColumnTotal(total: Long): CountBuilder =
    this.modify(_.count.perColumn).setTo(Some(perColCount.total(total).perColumnCount))

  def perColumnGenerator(generator: GeneratorBuilder): CountBuilder =
    this.modify(_.count.perColumn).setTo(Some(perColCount.generator(generator).perColumnCount))

  private def perColCount: PerColumnCountBuilder = {
    count.perColumn match {
      case Some(value) => PerColumnCountBuilder(value)
      case None => PerColumnCountBuilder()
    }
  }
}

case class PerColumnCountBuilder(perColumnCount: PerColumnCount = PerColumnCount()) {
  def total(total: Long): PerColumnCountBuilder =
    this.modify(_.perColumnCount.count).setTo(Some(total))

  def generator(generator: GeneratorBuilder): PerColumnCountBuilder =
    this.modify(_.perColumnCount.generator).setTo(Some(generator.generator))

  def columns(columns: String*): PerColumnCountBuilder =
    this.modify(_.perColumnCount.columnNames).setTo(columns.toList)
}

case class SchemaBuilder(schema: Schema = Schema()) {
  def addField(name: String, `type`: String): SchemaBuilder =
    addFields(FieldBuilder().name(name).`type`(`type`))

  def addField(field: FieldBuilder): SchemaBuilder =
    addFields(field)

  def addFields(fields: FieldBuilder*): SchemaBuilder =
    this.modify(_.schema.fields).setTo(schema.fields match {
      case Some(value) => Some(value ++ fields.map(_.field))
      case None => Some(fields.map(_.field).toList)
    })
}

case class FieldBuilder(field: Field = Field()) {
  def name(name: String): FieldBuilder =
    this.modify(_.field.name).setTo(name)

  def `type`(`type`: String): FieldBuilder =
    this.modify(_.field.`type`).setTo(Some(`type`))

  def nullable(nullable: Boolean): FieldBuilder =
    this.modify(_.field.nullable).setTo(nullable)

  def generator(generator: GeneratorBuilder): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(generator.generator))

  def random: FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.random.generator))

  def sql(sql: String): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.sql(sql).generator))

  def regex(regex: String): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.regex(regex).generator))

  def oneOf(values: Any*): FieldBuilder =
    this.modify(_.field.generator).setTo(Some(getGenBuilder.oneOf(values).generator))
      .modify(_.field.`type`)
      .setTo(
        values.head match {
          case _: Double => Some("double")
          case _: String => Some("string")
          case _: Long | Int => Some("long")
          case _: Boolean => Some("boolean")
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
  def random: GeneratorBuilder = this.modify(_.generator.`type`).setTo(RANDOM_GENERATOR)

  def sql(sql: String): GeneratorBuilder = this.modify(_.generator.`type`).setTo(SQL_GENERATOR)
    .modify(_.generator.options)(_ ++ Map(SQL_GENERATOR -> sql))

  def regex(regex: String): GeneratorBuilder = this.modify(_.generator.`type`).setTo(REGEX_GENERATOR)
    .modify(_.generator.options)(_ ++ Map(REGEX_GENERATOR -> regex))

  def oneOf(values: Any*): GeneratorBuilder =
    this.modify(_.generator.`type`).setTo(ONE_OF_GENERATOR)
      .modify(_.generator.options)(_ ++ Map(ONE_OF_GENERATOR -> values))

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