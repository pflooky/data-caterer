package com.github.pflooky.datacaterer.api.model

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.spark.sql.types.{ArrayType, DataType, Metadata, MetadataBuilder, StructField, StructType}

import scala.language.implicitConversions

case class Plan(
                 name: String = "Default plan",
                 description: String = "Data generation plan",
                 tasks: List[TaskSummary] = List(),
                 sinkOptions: Option[SinkOptions] = None,
                 validations: List[String] = List()
               )

case class SinkOptions(
                        seed: Option[String] = None,
                        locale: Option[String] = None,
                        foreignKeys: Map[String, List[String]] = Map()
                      )

case class ForeignKeyRelation(
                               dataSource: String = "default_data_source",
                               step: String = "default_step",
                               column: String = "default_column"
                             ) {
  override def toString: String = s"$dataSource.$step.$column"
}

case class TaskSummary(
                        name: String,
                        dataSourceName: String,
                        enabled: Boolean = true
                      )

case class Task(
                 name: String = "default_task",
                 steps: List[Step] = List()
               )

case class Step(
                 name: String = "default_step",
                 `type`: String = "json",
                 count: Count = Count(),
                 options: Map[String, String] = Map(),
                 schema: Schema = Schema(),
                 enabled: Boolean = true
               )

case class Count(
                  @JsonDeserialize(contentAs = classOf[java.lang.Long]) total: Option[Long] = Some(1000L),
                  perColumn: Option[PerColumnCount] = None,
                  generator: Option[Generator] = None
                )

case class PerColumnCount(
                           columnNames: List[String] = List(),
                           @JsonDeserialize(contentAs = classOf[java.lang.Long]) count: Option[Long] = Some(10L),
                           generator: Option[Generator] = None
                         )

case class Schema(
                   fields: Option[List[Field]] = None
                 )

case class Field(
                  name: String = "default_field",
                  `type`: Option[String] = Some("string"),
                  generator: Option[Generator] = Some(Generator()),
                  nullable: Boolean = true,
                  static: Option[String] = None,
                  schema: Option[Schema] = None
                )

case class Generator(
                      `type`: String = "random",
                      options: Map[String, Any] = Map()
                    )
