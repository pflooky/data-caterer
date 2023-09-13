package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder
import com.github.pflooky.datacaterer.api.model.{ForeignKeyRelation, Plan, SinkOptions}
import com.softwaremill.quicklens.ModifyPimp

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

  def sinkOptions(sinkOptions: SinkOptions): PlanBuilder =
    this.modify(_.plan.sinkOptions).setTo(Some(sinkOptions))

  def seed(seed: Long): PlanBuilder =
    this.modify(_.plan.sinkOptions).setTo(Some(getSinkOpt.seed(seed).sinkOptions))

  def locale(locale: String): PlanBuilder =
    this.modify(_.plan.sinkOptions).setTo(Some(getSinkOpt.locale(locale).sinkOptions))

  def addForeignKeyRelationship(foreignKey: ForeignKeyRelation, relation: ForeignKeyRelation, relations: ForeignKeyRelation*): PlanBuilder =
    addForeignKeyRelationship(foreignKey, (relation +: relations).toList)

  def addForeignKeyRelationship(foreignKey: ForeignKeyRelation, relations: List[ForeignKeyRelation]): PlanBuilder =
    this.modify(_.plan.sinkOptions).setTo(Some(getSinkOpt.foreignKey(foreignKey, relations).sinkOptions))

  def addForeignKeyRelationship(connectionTaskBuilder: ConnectionTaskBuilder[_], column: String,
                                relations: List[(ConnectionTaskBuilder[_], String)]): PlanBuilder = {
    val baseRelation = toForeignKeyRelation(connectionTaskBuilder, column)
    val otherRelations = relations.map(r => toForeignKeyRelation(r._1, r._2))
    this.modify(_.plan.sinkOptions).setTo(Some(getSinkOpt.foreignKey(baseRelation, otherRelations).sinkOptions))
  }

  private def toForeignKeyRelation(connectionTaskBuilder: ConnectionTaskBuilder[_], column: String) = {
    val dataSource = connectionTaskBuilder.connectionConfigWithTaskBuilder.dataSourceName
    connectionTaskBuilder.step match {
      case Some(value) =>
        val hasColumn = value.step.schema.fields.getOrElse(List()).exists(f => f.name == column)
        if (!hasColumn) {
          throw new RuntimeException(s"Column name defined in foreign key relationship does not exist, data-source=$dataSource, column-name=$column")
        }
        ForeignKeyRelation(dataSource, value.step.name, column)
      case None =>
        throw new RuntimeException(s"Not schema defined for data source. Cannot create foreign key relationship, data-source=$dataSource, column-name=$column")
    }
  }

  private def getSinkOpt: SinkOptionsBuilder = {
    plan.sinkOptions match {
      case Some(value) => SinkOptionsBuilder(value)
      case None => SinkOptionsBuilder()
    }
  }
}
