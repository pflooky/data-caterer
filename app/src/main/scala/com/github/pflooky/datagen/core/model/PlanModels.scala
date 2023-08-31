package com.github.pflooky.datagen.core.model

import com.github.pflooky.datacaterer.api.model.Constants.{IS_PRIMARY_KEY, IS_UNIQUE, MAXIMUM, MINIMUM, ONE_OF_GENERATOR, PRIMARY_KEY_POSITION, RANDOM_GENERATOR, STATIC}
import com.github.pflooky.datacaterer.api.model.{Count, Field, ForeignKeyRelation, Generator, PerColumnCount, Schema, SinkOptions, Step, Task}
import com.github.pflooky.datagen.core.exception.{ForeignKeyFormatException, InvalidFieldConfigurationException}
import com.github.pflooky.datagen.core.generator.metadata.datasource.DataSourceDetail
import com.github.pflooky.datagen.core.util.{MetadataUtil, ObjectMapperUtil}
import org.apache.spark.sql.types.{ArrayType, DataType, Metadata, MetadataBuilder, StructField, StructType}

import scala.language.implicitConversions


object ForeignKeyRelationHelper {
  def fromString(str: String): ForeignKeyRelation = {
    val strSpt = str.split("\\.")
    if (strSpt.length == 2) {
      ForeignKeyRelation(strSpt.head, strSpt.last, "")
    } else if (strSpt.length != 3) {
      throw new ForeignKeyFormatException(str)
    } else ForeignKeyRelation(strSpt.head, strSpt(1), strSpt.last)
  }
}

object TaskHelper {
  def fromMetadata(name: String, stepType: String, structTypes: List[DataSourceDetail]): Task = {
    val steps = structTypes.zipWithIndex.map(structType => {
      Step(structType._1.dataSourceMetadata.toStepName(structType._1.sparkOptions), stepType, Count(), structType._1.sparkOptions, SchemaHelper.fromStructType(structType._1.structType))
    })
    Task(name, steps)
  }
}

object SchemaHelper {

  def fromStructType(structType: StructType): Schema = {
    val fields = structType.fields.map(FieldHelper.fromStructField).toList
    Schema(Some(fields))
  }
}

object FieldHelper {

  def fromStructField(structField: StructField): Field = {
    val metadataOptions = MetadataUtil.metadataToMap(structField.metadata)
    val generator = if (structField.metadata.contains(ONE_OF_GENERATOR)) {
      Generator(ONE_OF_GENERATOR, metadataOptions)
    } else {
      Generator(RANDOM_GENERATOR, metadataOptions)
    }
    Field(structField.name, Some(structField.dataType.sql.toLowerCase), Some(generator), structField.nullable)
  }
}

object PlanImplicits {

  implicit class ForeignKeyRelationOps(foreignKeyRelation: ForeignKeyRelation) {
    def dataFrameName = s"${foreignKeyRelation.dataSource}.${foreignKeyRelation.step}"
  }

  implicit class SinkOptionsOps(sinkOptions: SinkOptions) {
    def gatherForeignKeyRelations(key: String): (ForeignKeyRelation, List[ForeignKeyRelation]) = {
      val source = ForeignKeyRelationHelper.fromString(key)
      val targets = sinkOptions.foreignKeys(key)
      val targetForeignKeys = targets.map(ForeignKeyRelationHelper.fromString)
      (source, targetForeignKeys)
    }

    def foreignKeysWithoutColumnNames: Map[String, List[String]] = {
      sinkOptions.foreignKeys.map(foreignKey => {
        val mainFk = foreignKey._1.split("\\.").take(2).mkString(".")
        val subFks = foreignKey._2.map(sFk => sFk.split("\\.").take(2).mkString("."))
        (mainFk, subFks)
      })
    }
  }

  implicit class TaskOps(task: Task) {
    def toTaskDetailString: String = {
      val enabledSteps = task.steps.filter(_.enabled)
      val stepSummary = enabledSteps.map(_.toStepDetailString).mkString(",")
      s"name=${task.name}, num-steps=${task.steps.size}, num-enabled-steps=${enabledSteps.size}, enabled-steps-summary=($stepSummary)"
    }
  }

  implicit class StepOps(step: Step) {
    def toStepDetailString: String = {
      s"name=${step.name}, type=${step.`type`}, options=${step.options}, step-num-records=(${step.count.numRecordsString}), schema-summary=(${step.schema.toString})"
    }

    def gatherPrimaryKeys: List[String] = {
      if (step.schema.fields.isDefined) {
        val fields = step.schema.fields.get
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
      step.schema.fields.map(fields => {
        fields.filter(field => {
          field.generator
            .flatMap(gen => gen.options.get(IS_UNIQUE).map(_.toString.toBoolean))
            .getOrElse(false)
        }).map(_.name)
      }).getOrElse(List())
    }
  }

  implicit class CountOps(count: Count) {
    def numRecordsString: String = {
      if (count.total.isDefined && count.perColumn.isDefined && count.perColumn.get.count.isDefined && count.perColumn.get.generator.isEmpty) {
        val records = count.total.get * count.perColumn.get.count.get
        s"per-column-count: columns=${count.perColumn.get.columnNames.mkString(",")}, num-records=${records.toString}"
      } else if (count.perColumn.isDefined && count.perColumn.get.generator.isDefined) {
        s"per-column-count: columns=${count.perColumn.get.columnNames.mkString(",")}, num-records-via-generator=(${count.perColumn.get.generator.get.toString})"
      } else if (count.total.isDefined) {
        s"basic-count: num-records=${count.total.get.toString}"
      } else if (count.generator.isDefined) {
        s"generated-count: num-records=${count.generator.toString}"
      } else {
        //TODO: should throw error here?
        "0"
      }
    }

    def numRecords: Long = {
      (count.total, count.generator, count.perColumn, count.perColumn.flatMap(_.generator)) match {
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

  implicit class PerColumnCountOps(perColumnCount: PerColumnCount) {
    def averageCountPerColumn: Long = {
      perColumnCount.generator.map(_.averageCount).getOrElse(perColumnCount.count.map(identity).getOrElse(1L))
    }
  }

  implicit class SchemaOps(schema: Schema) {
    def toStructType: StructType = {
      if (schema.fields.isDefined) {
        val structFields = schema.fields.get.map(_.toStructField)
        StructType(structFields)
      } else {
        StructType(Seq())
      }
    }
  }

  implicit class FieldOps(field: Field) {
    def toStructField: StructField = {
      if (field.static.isDefined) {
        val metadata = new MetadataBuilder().putString(STATIC, field.static.get).build()
        StructField(field.name, DataType.fromDDL(field.`type`.get), field.nullable, metadata)
      } else if (field.schema.isDefined) {
        val innerStructFields = field.schema.get.toStructType
        StructField(
          field.name,
          if (field.`type`.isDefined && field.`type`.get.toLowerCase.startsWith("array")) ArrayType(innerStructFields, field.nullable) else innerStructFields,
          field.nullable
        )
      } else if (field.generator.isDefined && field.`type`.isDefined) {
        val metadata = Metadata.fromJson(ObjectMapperUtil.jsonObjectMapper.writeValueAsString(field.generator.get.options))
        StructField(field.name, DataType.fromDDL(field.`type`.get), field.nullable, metadata)
      } else {
        throw new InvalidFieldConfigurationException(this.field)
      }
    }
  }

  implicit class GeneratorOps(generator: Generator) {
    def averageCount: Long = {
      if (generator.`type`.equalsIgnoreCase(RANDOM_GENERATOR)) {
        val min = generator.options.get(MINIMUM).map(_.toString.toLong).getOrElse(1L)
        val max = generator.options.get(MAXIMUM).map(_.toString.toLong).getOrElse(10L)
        (max + min + 1) / 2
      } else 1L
    }
  }
}
