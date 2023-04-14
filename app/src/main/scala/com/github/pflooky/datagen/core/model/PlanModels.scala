package com.github.pflooky.datagen.core.model

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.github.pflooky.datagen.core.exception.ForeignKeyFormatException

case class Plan(name: String, description: String, tasks: List[TaskSummary], sinkOptions: Option[SinkOptions] = None)

case class SinkOptions(seed: Option[String], foreignKeys: Map[String, List[String]] = Map()) {
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

case class Task(name: String, steps: List[Step]) {
  def toTaskDetailString: String = {
    val enabledSteps = steps.filter(_.enabled)
    val stepSummary = enabledSteps.map(_.toStepDetailString).mkString(",")
    s"name=$name, num-steps=${steps.size}, num-enabled-steps=${enabledSteps.size}, enabled-steps-summary=($stepSummary)"
  }
}

case class Step(name: String, `type`: String, count: Count, options: Map[String, String] = Map(), schema: Schema, enabled: Boolean = true) {
  def toStepDetailString: String = {
    s"name=$name, type=${`type`}, options=$options, step-num-records=(${count.getNumRecordsString}), schema-summary=(${schema.toString})"
  }
}

case class Count(@JsonDeserialize(contentAs = classOf[java.lang.Long]) total: Option[Long] = Some(1000L), perColumn: Option[PerColumnCount] = None, generator: Option[Generator] = None) {
  def getNumRecordsString: String = {
    if (total.isDefined && perColumn.isDefined && perColumn.get.count.isDefined && perColumn.get.generator.isEmpty) {
      val records = total.get * perColumn.get.count.get
      s"per-column-count: columns=${perColumn.get.columnNames.mkString(",")}, num-records=${records.toString}"
    } else if (perColumn.isDefined && perColumn.get.generator.isDefined) {
      s"per-column-count: columns=${perColumn.get.columnNames.mkString(",")}, num-records-via-generator=(${perColumn.get.generator.get.toString})"
    } else if (total.isDefined) {
      s"basic-count: num-records=${total.get.toString}"
    } else if (generator.isDefined) {
      s"generated-count: num-records=${generator.toString}"
    } else {
      //TODO: should throw error here?
      "0"
    }
  }
}
case class PerColumnCount(columnNames: List[String], @JsonDeserialize(contentAs = classOf[java.lang.Long]) count: Option[Long] = Some(10L), generator: Option[Generator] = None)

case class Schema(`type`: String, fields: Option[List[Field]]) {
  override def toString: String = {
    val baseStr = s"type=${`type`}"
    if (fields.isDefined) {
      baseStr + s", num-fields=${fields.get.size}"
    } else baseStr
  }
}
case class Field(name: String, `type`: String, generator: Generator, nullable: Boolean = false, defaultValue: Option[Any] = None)

case class Generator(`type`: String, options: Map[String, Any] = Map()) {
  override def toString: String = {
    s"type=${`type`}, options=$options"
  }
}
