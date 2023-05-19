package com.github.pflooky.datagen.core.model

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.github.pflooky.datagen.core.exception.ForeignKeyFormatException
import com.github.pflooky.datagen.core.generator.plan.datasource.DataSourceDetail
import com.github.pflooky.datagen.core.model.Constants.{ARRAY_NESTED_FIELD_NAME_DELIMITER, GENERATED, NESTED_FIELD_NAME_DELIMITER, RANDOM}
import com.github.pflooky.datagen.core.util.MetadataUtil
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.implicitConversions

case class Plan(name: String, description: String, tasks: List[TaskSummary], sinkOptions: Option[SinkOptions] = None)

case class SinkOptions(seed: Option[String], locale: Option[String], foreignKeys: Map[String, List[String]] = Map()) {
  def getForeignKeyRelations(key: String): (ForeignKeyRelation, List[ForeignKeyRelation]) = {
    val source = ForeignKeyRelation.fromString(key)
    val targets = foreignKeys(key)
    val targetForeignKeys = targets.map(ForeignKeyRelation.fromString)
    (source, targetForeignKeys)
  }
}
case class ForeignKeyRelation(dataSource: String, step: String, column: String) {
  override def toString: String = s"$dataSource.$step.$column"

  def getDataFrameName = s"$dataSource.$step"
}

object ForeignKeyRelation {
  def fromString(str: String): ForeignKeyRelation = {
    val strSpt = str.split("\\.")
    if (strSpt.length != 3) throw new ForeignKeyFormatException(str)
    ForeignKeyRelation(strSpt.head, strSpt(1), strSpt.last)
  }
}

case class TaskSummary(name: String, dataSourceName: String, enabled: Boolean = true)

case class Task(name: String, steps: List[Step]) {
  def toTaskDetailString: String = {
    val enabledSteps = steps.filter(_.enabled)
    val stepSummary = enabledSteps.map(_.toStepDetailString).mkString(",")
    s"name=$name, num-steps=${steps.size}, num-enabled-steps=${enabledSteps.size}, enabled-steps-summary=($stepSummary)"
  }
}

object Task {
  implicit def fromMetadata(name: String, stepType: String, structTypes: List[DataSourceDetail]): Task = {
    val steps = structTypes.zipWithIndex.map(structType => {
      Step(structType._1.dataSourceMetadata.toStepName(structType._1.sparkOptions), stepType, Count(), structType._1.sparkOptions, Schema.fromStructType(GENERATED, structType._1.structType))
    })
    Task(name, steps)
  }
}

case class Step(name: String, `type`: String, count: Count, options: Map[String, String] = Map(), schema: Schema, enabled: Boolean = true) {
  def toStepDetailString: String = {
    s"name=$name, type=${`type`}, options=$options, step-num-records=(${count.numRecordsString}), schema-summary=(${schema.toString})"
  }
}

case class Count(@JsonDeserialize(contentAs = classOf[java.lang.Long]) total: Option[Long] = Some(1000L), perColumn: Option[PerColumnCount] = None, generator: Option[Generator] = None) {
  def numRecordsString: String = {
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

object Schema {
  implicit def fromStructType(schemaType: String, structType: StructType): Schema = {
    val fields = structType.fields.map(Field.fromStructField).toList
    Schema(schemaType, Some(fields))
  }

  def flattenFields(listFields: List[Field]): List[Field] = {
    listFields.flatMap(field => {
      (field.schema, field.`type`) match {
        case (Some(schema), Some("array")) =>
          schema.fields
            .map(schemaFields =>
              flattenFields(schemaFields.map(f => f.copy(name = s"${field.name}$ARRAY_NESTED_FIELD_NAME_DELIMITER${f.name}")))
            ).getOrElse(List())
        case (Some(schema), _) =>
          schema.fields
            .map(schemaFields =>
              flattenFields(
                schemaFields.map(f => f.copy(name = s"${field.name}$NESTED_FIELD_NAME_DELIMITER${f.name}"))
              )).getOrElse(List())
        case _ =>
          List(field)
      }
    })
  }

  def unwrapNestedFields(df: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    val unwrappedFields = Schema.unwrapFields(df.schema.fields)
    val selectExpr = unwrappedFields.map(field => {
      field.dataType match {
        case structType: StructType =>
          val mapFields = fieldToNestedFieldsString(field, structType)
          s"named_struct($mapFields) AS ${field.name}"
        case arrayType: ArrayType =>
          arrayType.elementType match {
            case structType: StructType =>
              val mapFields = fieldToNestedFieldsString(field, structType, ARRAY_NESTED_FIELD_NAME_DELIMITER)
              s"array(named_struct($mapFields)) AS ${field.name}"
            case _ =>
              s"array(${field.name})"
          }
        case _ =>
          field.name
      }
    })
    val unwrappedDf = df.selectExpr(selectExpr: _*)
    sparkSession.createDataFrame(unwrappedDf.toJavaRDD, StructType(unwrappedFields))
  }

  def unwrapFields(fields: Array[StructField]): Array[StructField] = {
    val baseFields = fields.filter(f => !f.name.contains(NESTED_FIELD_NAME_DELIMITER) && !f.name.contains(ARRAY_NESTED_FIELD_NAME_DELIMITER))
    val arrayNestedFields = fields.filter(field => field.name.contains(ARRAY_NESTED_FIELD_NAME_DELIMITER))
      .map(f => {
        val spt = f.name.split(ARRAY_NESTED_FIELD_NAME_DELIMITER)
        (spt.head, f.copy(name = spt.tail.mkString(ARRAY_NESTED_FIELD_NAME_DELIMITER)))
      })
      .groupBy(_._1)
      .map(groupedFields => StructField(groupedFields._1, ArrayType(StructType(unwrapFields(groupedFields._2.map(_._2))))))

    val nestedFields = fields.filter(field => field.name.contains(NESTED_FIELD_NAME_DELIMITER))
      .map(f => {
        val spt = f.name.split("\\|\\|")
        (spt.head, f.copy(name = spt.tail.mkString(NESTED_FIELD_NAME_DELIMITER)))
      })
      .groupBy(_._1)
      .map(groupedFields => {
        StructField(groupedFields._1, StructType(unwrapFields(groupedFields._2.map(_._2))))
      }).toArray
    baseFields ++ nestedFields ++ arrayNestedFields
  }

  private def fieldToNestedStructString(field: StructField): String = {
    val cleanName = field.name.split("\\|\\||>>").last
    field.dataType match {
      case structType: StructType =>
        val mapFields = fieldToNestedFieldsString(field, structType)
        s"'$cleanName', named_struct($mapFields)"
      case _ =>
        s"'$cleanName', CAST(`${field.name}` AS ${field.dataType.sql})"
    }
  }

  private def fieldToNestedFieldsString(baseField: StructField, structType: StructType, delimiter: String = NESTED_FIELD_NAME_DELIMITER): String = {
    structType.fields.map(f => fieldToNestedStructString(f.copy(name = s"${baseField.name}$delimiter${f.name}"))).mkString(",")
  }
}

case class Field(name: String, `type`: Option[String] = None, generator: Option[Generator] = None, nullable: Boolean = false, defaultValue: Option[Any] = None, schema: Option[Schema] = None)

object Field {
  implicit def fromStructField(structField: StructField): Field = {
    Field(structField.name, Some(structField.dataType.typeName), Some(Generator(RANDOM, MetadataUtil.toMap(structField.metadata))), structField.nullable)
  }
}

case class Generator(`type`: String, options: Map[String, Any] = Map()) {
  override def toString: String = {
    s"type=${`type`}, options=$options"
  }
}
