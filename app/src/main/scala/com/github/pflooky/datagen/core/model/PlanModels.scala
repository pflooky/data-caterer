package com.github.pflooky.datagen.core.model

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.github.pflooky.datagen.core.exception.{ForeignKeyFormatException, InvalidFieldConfigurationException}
import com.github.pflooky.datagen.core.generator.metadata.datasource.DataSourceDetail
import com.github.pflooky.datagen.core.model.Constants.{GENERATED, IS_PRIMARY_KEY, IS_UNIQUE, MAXIMUM, MINIMUM, ONE_OF_GENERATOR, PRIMARY_KEY_POSITION, RANDOM_GENERATOR, STATIC}
import com.github.pflooky.datagen.core.util.{MetadataUtil, ObjectMapperUtil}
import org.apache.spark.sql.types.{ArrayType, DataType, Metadata, MetadataBuilder, StructField, StructType}

import scala.language.implicitConversions

case class Plan(name: String, description: String = "Data generation plan", tasks: List[TaskSummary] = List(), sinkOptions: Option[SinkOptions] = None, validations: List[String] = List())

case class SinkOptions(seed: Option[String] = None, locale: Option[String] = None, foreignKeys: Map[String, List[String]] = Map()) {
  def gatherForeignKeyRelations(key: String): (ForeignKeyRelation, List[ForeignKeyRelation]) = {
    val source = ForeignKeyRelation.fromString(key)
    val targets = foreignKeys(key)
    val targetForeignKeys = targets.map(ForeignKeyRelation.fromString)
    (source, targetForeignKeys)
  }

  def foreignKeysWithoutColumnNames: Map[String, List[String]] = {
    foreignKeys.map(foreignKey => {
      val mainFk = foreignKey._1.split("\\.").take(2).mkString(".")
      val subFks = foreignKey._2.map(sFk => sFk.split("\\.").take(2).mkString("."))
      (mainFk, subFks)
    })
  }
}
case class ForeignKeyRelation(dataSource: String, step: String, column: String) {
  override def toString: String = s"$dataSource.$step.$column"

  def dataFrameName = s"$dataSource.$step"
}

object ForeignKeyRelation {
  def fromString(str: String): ForeignKeyRelation = {
    val strSpt = str.split("\\.")
    if (strSpt.length == 2) {
      ForeignKeyRelation(strSpt.head, strSpt.last, "")
    } else if (strSpt.length != 3) {
      throw new ForeignKeyFormatException(str)
    } else ForeignKeyRelation(strSpt.head, strSpt(1), strSpt.last)
  }
}

case class TaskSummary(name: String, dataSourceName: String, enabled: Boolean = true)

case class Task(name: String, steps: List[Step] = List()) {
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

case class Step(name: String, `type`: String = "json", count: Count = Count(), options: Map[String, String] = Map(), schema: Schema = Schema(), enabled: Boolean = true) {
  def toStepDetailString: String = {
    s"name=$name, type=${`type`}, options=$options, step-num-records=(${count.numRecordsString}), schema-summary=(${schema.toString})"
  }

  def gatherPrimaryKeys: List[String] = {
    if (schema.fields.isDefined) {
      val fields = schema.fields.get
      fields.filter(field => {
        if (field.generator.isDefined) {
          val metadata = field.generator.get.options
          metadata.contains(IS_PRIMARY_KEY) && metadata(IS_PRIMARY_KEY).toString.toBoolean
        } else false
      })
        .map(field => (field.name, field.generator.get.options.getOrElse(PRIMARY_KEY_POSITION, "1").toString.toInt))
        .sortBy(_._2)
        .map(_._1)
    } else List()
  }

  def gatherUniqueFields: List[String] = {
    schema.fields.map(fields => {
      fields.filter(field => {
        field.generator
          .flatMap(gen => gen.options.get(IS_UNIQUE).map(_.toString.toBoolean))
          .getOrElse(false)
      }).map(_.name)
    }).getOrElse(List())
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

  def numRecords: Long = {
    (total, generator, perColumn, perColumn.flatMap(_.generator)) match {
      case (Some(t), None, Some(perCol), Some(_)) =>
        perCol.averageCountPerColumn * t
      case (Some(t), None, Some(perCol), None) =>
        perCol.count.get * t
      case (Some(t), Some(gen), None, None) =>
        gen.averageCount * t
      case (None, Some(gen), None, None) =>
        gen.averageCount
      case (Some(t), None, None, None) =>
        t
      case _ => 1000L
    }
  }
}
case class PerColumnCount(columnNames: List[String] = List(), @JsonDeserialize(contentAs = classOf[java.lang.Long]) count: Option[Long] = Some(10L), generator: Option[Generator] = None) {

  def averageCountPerColumn: Long = {
    generator.map(_.averageCount).getOrElse(count.map(identity).getOrElse(1L))
  }
}

case class Schema(`type`: String = "manual", fields: Option[List[Field]] = None) {
  override def toString: String = {
    val baseStr = s"type=${`type`}"
    if (fields.isDefined) {
      baseStr + s", num-fields=${fields.get.size}"
    } else baseStr
  }

  def toStructType: StructType = {
    if (fields.isDefined) {
      val structFields = fields.get.map(_.toStructField)
      StructType(structFields)
    } else {
      StructType(Seq())
    }
  }
}

object Schema {
  implicit def fromStructType(schemaType: String, structType: StructType): Schema = {
    val fields = structType.fields.map(Field.fromStructField).toList
    Schema(schemaType, Some(fields))
  }
}

case class Field(name: String, `type`: Option[String] = Some("string"), generator: Option[Generator] = Some(Generator()),
                 nullable: Boolean = true, static: Option[String] = None, schema: Option[Schema] = None) {
  def toStructField: StructField = {
    if (static.isDefined) {
      val metadata = new MetadataBuilder().putString(STATIC, static.get).build()
      StructField(name, DataType.fromDDL(`type`.get), nullable, metadata)
    } else if (schema.isDefined) {
      val innerStructFields = schema.get.toStructType
      if (`type`.isDefined && `type`.get.toLowerCase.startsWith("array")) {
        StructField(name, ArrayType(innerStructFields, nullable), nullable)
      } else {
        StructField(name, innerStructFields, nullable)
      }
    } else if (generator.isDefined && `type`.isDefined) {
      val metadata = Metadata.fromJson(ObjectMapperUtil.jsonObjectMapper.writeValueAsString(generator.get.options))
      StructField(name, DataType.fromDDL(`type`.get), nullable, metadata)
    } else {
      throw new InvalidFieldConfigurationException(this)
    }
  }
}

object Field {
  implicit def fromStructField(structField: StructField): Field = {
    val metadataOptions = MetadataUtil.metadataToMap(structField.metadata)
    val generator = if (structField.metadata.contains(ONE_OF_GENERATOR)) {
      Generator(ONE_OF_GENERATOR, metadataOptions)
    } else {
      Generator(RANDOM_GENERATOR, metadataOptions)
    }
    Field(structField.name, Some(structField.dataType.sql.toLowerCase), Some(generator), structField.nullable)
  }
}

case class Generator(`type`: String = "random", options: Map[String, Any] = Map()) {
  override def toString: String = {
    s"type=${`type`}, options=$options"
  }

  def averageCount: Long = {
    if (`type`.equalsIgnoreCase(RANDOM_GENERATOR)) {
      val min = options.get(MINIMUM).map(_.toString.toLong).getOrElse(1L)
      val max = options.get(MAXIMUM).map(_.toString.toLong).getOrElse(10L)
      (max + min + 1) / 2
    } else 1L
  }
}
