package com.github.pflooky.datagen.core.model

import com.github.pflooky.datagen.core.exception.ForeignKeyFormatException

case class Plan(name: String, description: String, tasks: List[TaskSummary], sinkOptions: Option[SinkOptions] = None)

case class SinkOptions(foreignKeys: Map[String, List[String]] = Map()) {
  def getForeignKeyRelations(key: String): (ForeignKeyRelation, List[ForeignKeyRelation]) = {
    val sourceSpt = key.split("\\.")
    if (sourceSpt.length != 3) throw new ForeignKeyFormatException(key)
    val source = ForeignKeyRelation(sourceSpt.head, sourceSpt(1), sourceSpt.last)

    val targets = foreignKeys(key)
    val targetForeignKeys = targets.map(t => {
      val targetSpt = t.split("\\.")
      if (targetSpt.length != 3) throw new ForeignKeyFormatException(t)
      ForeignKeyRelation(targetSpt.head, targetSpt(1), targetSpt.last)
    })
    (source, targetForeignKeys)
  }
}
case class ForeignKeyRelation(sink: String, step: String, column: String) {
  def getDataFrameName = s"${sink}.$step"
}

case class TaskSummary(name: String, sinkName: String, enabled: Boolean = true)

case class Task(name: String, steps: List[Step])

case class Step(name: String, `type`: String, count: Count, options: Map[String, String] = Map(), schema: Schema, enabled: Boolean = true)

case class Count(total: Long = 1000, perColumn: Option[PerColumnCount] = None, generator: Option[Generator] = None)
case class PerColumnCount(columnNames: List[String], count: Long = 10, generator: Option[Generator] = None)

case class Schema(`type`: String, fields: Option[List[Field]])
case class Field(name: String, `type`: String, generator: Generator, nullable: Boolean = false, defaultValue: Option[Any] = None)

case class Generator(`type`: String, options: Map[String, Any] = Map())
