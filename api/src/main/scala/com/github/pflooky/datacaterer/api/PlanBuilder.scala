package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.model.{DataCatererConfiguration, ForeignKeyRelation, Plan, Task, ValidationConfiguration}
import com.softwaremill.quicklens.ModifyPimp


trait PlanRun {
  var _plan: Plan = Plan()
  var _tasks: List[Task] = List()
  var _configuration: DataCatererConfiguration = DataCatererConfiguration()
  var _validations: List[ValidationConfiguration] = List()

  def plan: PlanBuilder = PlanBuilder()

  def taskSummary: TaskSummaryBuilder = TaskSummaryBuilder()

  def tasks: TasksBuilder = TasksBuilder()

  def task: TaskBuilder = TaskBuilder()

  def step: StepBuilder = StepBuilder()

  def schema: SchemaBuilder = SchemaBuilder()

  def field: FieldBuilder = FieldBuilder()

  def generator: GeneratorBuilder = GeneratorBuilder()

  def count: CountBuilder = CountBuilder()

  def configuration: DataCatererConfigurationBuilder = DataCatererConfigurationBuilder()

  def validation: ValidationBuilder = ValidationBuilder()

  def dataSourceValidation: DataSourceValidationBuilder = DataSourceValidationBuilder()

  def validationConfig: ValidationConfigurationBuilder = ValidationConfigurationBuilder()

  def foreignField(dataSource: String, step: String, column: String): ForeignKeyRelation = ForeignKeyRelation(dataSource, step, column)

  def execute(tasks: TasksBuilder): Unit = execute(List(tasks))

  def execute(planBuilder: PlanBuilder, configuration: DataCatererConfigurationBuilder): Unit = {
    execute(planBuilder.tasks, planBuilder, configuration)
  }

  def execute(
               tasks: List[TasksBuilder] = List(),
               plan: PlanBuilder = PlanBuilder(),
               configuration: DataCatererConfigurationBuilder = DataCatererConfigurationBuilder(),
               validations: List[ValidationConfigurationBuilder] = List()
             ): Unit = {
    val taskToDataSource = tasks.flatMap(_.tasks.map(t => (t.task.name, t.dataSourceName, t.task)))
    val planWithTaskToDataSource = plan.taskSummaries(taskToDataSource.map(t => taskSummary.name(t._1).dataSourceName(t._2)): _*)

    _plan = planWithTaskToDataSource.plan
    _tasks = taskToDataSource.map(_._3)
    _configuration = configuration.dataCatererConfiguration
    _validations = validations.map(_.validationConfiguration)
  }
}

case class PlanBuilder(plan: Plan = Plan(), tasks: List[TasksBuilder] = List()) {

  def name(name: String): PlanBuilder =
    this.modify(_.plan.name).setTo(name)

  def description(desc: String): PlanBuilder =
    this.modify(_.plan.description).setTo(desc)

  def taskSummaries(taskSummaries: TaskSummaryBuilder*): PlanBuilder = {
    val tasksToAdd = taskSummaries.filter(_.task.isDefined)
      .map(x => TasksBuilder(List(x.task.get), x.taskSummary.dataSourceName))
      .toList
    this.modify(_.plan.tasks)(_ ++ taskSummaries.map(_.taskSummary))
      .modify(_.tasks)(_ ++ tasksToAdd)
  }

  def sinkOptions(sinkOptionsBuilder: SinkOptionsBuilder): PlanBuilder =
    this.modify(_.plan.sinkOptions).setTo(Some(sinkOptionsBuilder.sinkOptions))

  def seed(seed: String): PlanBuilder =
    this.modify(_.plan.sinkOptions).setTo(Some(getSinkOpt.seed(seed).sinkOptions))

  def locale(locale: String): PlanBuilder =
    this.modify(_.plan.sinkOptions).setTo(Some(getSinkOpt.locale(locale).sinkOptions))

  def addForeignKeyRelationship(foreignKey: ForeignKeyRelation, relations: ForeignKeyRelation*): PlanBuilder =
    addForeignKeyRelationship(foreignKey, relations.toList)

  def addForeignKeyRelationship(foreignKey: ForeignKeyRelation, relations: List[ForeignKeyRelation]): PlanBuilder =
    this.modify(_.plan.sinkOptions).setTo(Some(getSinkOpt.foreignKey(foreignKey, relations: _*).sinkOptions))

  private def getSinkOpt: SinkOptionsBuilder = {
    plan.sinkOptions match {
      case Some(value) => SinkOptionsBuilder(value)
      case None => SinkOptionsBuilder()
    }
  }
}
