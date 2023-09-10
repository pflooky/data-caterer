package com.github.pflooky.datacaterer.api

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

  private def getSinkOpt: SinkOptionsBuilder = {
    plan.sinkOptions match {
      case Some(value) => SinkOptionsBuilder(value)
      case None => SinkOptionsBuilder()
    }
  }
}
