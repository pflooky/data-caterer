package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.model.{ForeignKeyRelation, SinkOptions}
import com.softwaremill.quicklens.ModifyPimp

case class SinkOptionsBuilder(sinkOptions: SinkOptions = SinkOptions()) {
  def seed(seed: Long): SinkOptionsBuilder = this.modify(_.sinkOptions.seed).setTo(Some(seed.toString))

  def locale(locale: String): SinkOptionsBuilder = this.modify(_.sinkOptions.locale).setTo(Some(locale))

  def foreignKey(foreignKey: ForeignKeyRelation, relation: ForeignKeyRelation, relations: ForeignKeyRelation*): SinkOptionsBuilder =
    this.modify(_.sinkOptions.foreignKeys)(_ ++ Map(foreignKey.toString -> (relation +: relations).map(_.toString).toList))

  def foreignKey(foreignKey: ForeignKeyRelation, relations: List[ForeignKeyRelation]): SinkOptionsBuilder =
    this.foreignKey(foreignKey, relations.head, relations.tail: _*)
}
