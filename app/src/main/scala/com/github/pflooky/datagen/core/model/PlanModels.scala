package com.github.pflooky.datagen.core.model

import com.github.pflooky.datacaterer.api.PlanRun
import com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder
import com.github.pflooky.datacaterer.api.model.Constants.{DEFAULT_FIELD_NULLABLE, FOREIGN_KEY_DELIMITER, FOREIGN_KEY_DELIMITER_REGEX, IS_PRIMARY_KEY, IS_UNIQUE, MAXIMUM, MINIMUM, ONE_OF_GENERATOR, PRIMARY_KEY_POSITION, RANDOM_GENERATOR, STATIC}
import com.github.pflooky.datacaterer.api.model.{Count, Field, ForeignKeyRelation, Generator, PerColumnCount, Schema, SinkOptions, Step, Task}
import com.github.pflooky.datagen.core.exception.InvalidFieldConfigurationException
import com.github.pflooky.datagen.core.generator.metadata.datasource.DataSourceDetail
import com.github.pflooky.datagen.core.model.Constants.{COUNT_BASIC, COUNT_COLUMNS, COUNT_GENERATED, COUNT_GENERATED_PER_COLUMN, COUNT_NUM_RECORDS, COUNT_PER_COLUMN, COUNT_TYPE}
import com.github.pflooky.datagen.core.util.{MetadataUtil, ObjectMapperUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{ArrayType, DataType, Metadata, MetadataBuilder, StructField, StructType}

import scala.language.implicitConversions


object ForeignKeyRelationHelper {
  def fromString(str: String): ForeignKeyRelation = {
    val strSpt = str.split(FOREIGN_KEY_DELIMITER_REGEX, 3)
    if (strSpt.length == 2) {
      ForeignKeyRelation(strSpt.head, strSpt.last, List())
    } else {
      ForeignKeyRelation(strSpt.head, strSpt(1), strSpt.last.split(",").toList)
    }
  }

  def updateForeignKeyName(stepNameMapping: Map[String, String], foreignKey: String): String = {
    val fkDataSourceStep = foreignKey.split(FOREIGN_KEY_DELIMITER_REGEX).take(2).mkString(FOREIGN_KEY_DELIMITER)
    stepNameMapping.get(fkDataSourceStep)
      .map(newName => foreignKey.replace(fkDataSourceStep, newName))
      .getOrElse(foreignKey)
  }
}

object TaskHelper {
  private val LOGGER = Logger.getLogger(getClass.getName)

  def fromMetadata(optPlanRun: Option[PlanRun], name: String, stepType: String, structTypes: List[DataSourceDetail]): (Task, Map[String, String]) = {
    val steps = structTypes.zipWithIndex.map(structType => enrichWithUserDefinedOptions(name, stepType, structType._1, optPlanRun))
    val mappedStepNames = steps.map(_._2).filter(_.isDefined).map(_.get).toMap
    (Task(name, steps.map(_._1)), mappedStepNames)
  }

  private def enrichWithUserDefinedOptions(
                                            name: String,
                                            stepType: String,
                                            generatedDetails: DataSourceDetail,
                                            optPlanRun: Option[PlanRun]
                                          ): (Step, Option[(String, String)]) = {
    val stepName = generatedDetails.dataSourceMetadata.toStepName(generatedDetails.sparkOptions)

    def stepWithOptNameMapping(matchingStepOptions: Seq[ConnectionTaskBuilder[_]]): Some[(ConnectionTaskBuilder[_], Option[(String, String)])] = {
      val matchStep = matchingStepOptions.head
      val optStepNameMapping = matchStep.step.map(s => {
        val baseStepName = s"${matchStep.connectionConfigWithTaskBuilder.dataSourceName}$FOREIGN_KEY_DELIMITER"
        (s"$baseStepName${s.step.name}", s"$baseStepName$stepName")
      })
      Some(matchStep, optStepNameMapping)
    }

    //check if there is any user defined step attributes that need to be used
    val optUserConf = if (optPlanRun.isDefined) {
      val matchingDataSourceConfig = optPlanRun.get._connectionTaskBuilders.filter(_.connectionConfigWithTaskBuilder.dataSourceName == name)
      if (matchingDataSourceConfig.size == 1) {
        stepWithOptNameMapping(matchingDataSourceConfig)
      } else if (matchingDataSourceConfig.size > 1) {
        //multiple matches, so have to match against step options as well if defined
        val matchingStepOptions = matchingDataSourceConfig.filter(dsConf => dsConf.step.isDefined && dsConf.step.get.step.options == generatedDetails.sparkOptions)
        if (matchingStepOptions.size == 1) {
          stepWithOptNameMapping(matchingStepOptions)
        } else {
          LOGGER.warn(s"Multiple definitions of same sub data source found. Will default to taking first definition, data-source-name=$name, step-name=$stepName")
          stepWithOptNameMapping(matchingStepOptions)
        }
      } else {
        None
      }
    } else {
      None
    }

    val count = optUserConf.flatMap(_._1.step.map(_.step.count)).getOrElse(Count())
    val optUserSchema = optUserConf.flatMap(_._1.step.map(_.step.schema))
    val generatedSchema = SchemaHelper.fromStructType(generatedDetails.structType)
    val mergedSchema = optUserSchema.map(userSchema => SchemaHelper.mergeSchemaInfo(generatedSchema, userSchema)).getOrElse(generatedSchema)
    (Step(stepName, stepType, count, generatedDetails.sparkOptions, mergedSchema), optUserConf.flatMap(_._2))
  }
}

object SchemaHelper {
  private val LOGGER = Logger.getLogger(getClass.getName)

  def fromStructType(structType: StructType): Schema = {
    val fields = structType.fields.map(FieldHelper.fromStructField).toList
    Schema(Some(fields))
  }

  /**
   * Merge the field definitions together, taking schema2 field definition as preference
   *
   * @param schema1 First schema all fields defined
   * @param schema2 Second schema which may have all or subset of fields defined where it will override if same
   *                options defined in schema1
   * @return Merged schema
   */
  def mergeSchemaInfo(schema1: Schema, schema2: Schema): Schema = {
    (schema1.fields, schema2.fields) match {
      case (Some(fields1), Some(fields2)) =>
        val mergedFields = fields1.map(field => {
          val filterInSchema2 = fields2.filter(f2 => f2.name == field.name)
          val optFieldToMerge = if (filterInSchema2.nonEmpty) {
            if (filterInSchema2.size > 1) {
              LOGGER.warn(s"Multiple field definitions found. Only taking the first definition, field-name=${field.name}")
            }
            Some(filterInSchema2.head)
          } else {
            None
          }
          optFieldToMerge.map(f2 => {
            val fieldSchema = (field.schema, f2.schema) match {
              case (Some(fSchema), Some(f2Schema)) => Some(mergeSchemaInfo(fSchema, f2Schema))
              case (Some(fSchema), None) => Some(fSchema)
              case (None, Some(_)) =>
                LOGGER.warn(s"Schema from metadata source or from data source has no nested schema for field but has nested schema defined by user. " +
                  s"Ignoring user defined nested schema, field-name=${field.name}")
                None
              case _ => None
            }
            val fieldType = mergeFieldType(field, f2)
            val fieldGenerator = mergeGenerator(field, f2)
            val fieldNullable = mergeNullable(field, f2)
            val fieldStatic = mergeStaticValue(field, f2)
            Field(field.name, fieldType, fieldGenerator, fieldNullable, fieldStatic, fieldSchema)
          }).getOrElse(field)
        })
        Schema(Some(mergedFields))
      case (Some(_), None) => schema1
      case (None, Some(_)) => schema2
      case _ =>
        throw new RuntimeException("Schema not defined from auto generation, metadata source or from user")
    }
  }

  private def mergeStaticValue(field: Field, f2: Field) = {
    (field.static, f2.static) match {
      case (Some(fStatic), Some(f2Static)) =>
        if (fStatic.equalsIgnoreCase(f2Static)) {
          field.static
        } else {
          LOGGER.warn(s"User has defined static value different to metadata source or from data source. " +
            s"Using user defined static value, field-name=${field.name}, user-static-value=$f2Static, data-static-value=$fStatic")
          f2.static
        }
      case (Some(_), None) => field.static
      case (None, Some(_)) => f2.static
      case _ => None
    }
  }

  private def mergeNullable(field: Field, f2: Field) = {
    (field.nullable, f2.nullable) match {
      case (false, _) => false
      case (true, false) => false
      case _ => DEFAULT_FIELD_NULLABLE
    }
  }

  private def mergeGenerator(field: Field, f2: Field) = {
    (field.generator, f2.generator) match {
      case (Some(fGen), Some(f2Gen)) =>
        val genType = if (fGen.`type`.equalsIgnoreCase(f2Gen.`type`)) fGen.`type` else f2Gen.`type`
        val options = fGen.options ++ f2Gen.options
        Some(Generator(genType, options))
      case (Some(_), None) => field.generator
      case (None, Some(_)) => f2.generator
      case _ => None
    }
  }

  private def mergeFieldType(field: Field, f2: Field) = {
    (field.`type`, f2.`type`) match {
      case (Some(fType), Some(f2Type)) =>
        if (fType.equalsIgnoreCase(f2Type)) {
          field.`type`
        } else {
          LOGGER.warn(s"User has defined data type different to metadata source or from data source. " +
            s"Using user defined type, field-name=${field.name}, user-type=$f2Type, data-source-type=$fType")
          f2.`type`
        }
      case (Some(_), None) => field.`type`
      case (None, Some(_)) => f2.`type`
      case _ => field.`type`
    }
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
      val targets = sinkOptions.foreignKeys.filter(f => f._1.equalsIgnoreCase(key)).flatMap(_._2)
      val targetForeignKeys = targets.map(ForeignKeyRelationHelper.fromString)
      (source, targetForeignKeys)
    }

    def foreignKeysWithoutColumnNames: List[(String, List[String])] = {
      sinkOptions.foreignKeys.map(foreignKey => {
        val mainFk = foreignKey._1.split(FOREIGN_KEY_DELIMITER_REGEX).take(2).mkString(FOREIGN_KEY_DELIMITER)
        val subFks = foreignKey._2.map(sFk => sFk.split(FOREIGN_KEY_DELIMITER_REGEX).take(2).mkString(FOREIGN_KEY_DELIMITER))
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
      s"name=${step.name}, type=${step.`type`}, options=${step.options}, step-num-records=(${step.count.numRecordsString._1}), schema-summary=(${step.schema.toString})"
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
    def numRecordsString: (String, List[List[String]]) = {
      if (count.records.isDefined && count.perColumn.isDefined && count.perColumn.get.count.isDefined && count.perColumn.get.generator.isEmpty) {
        val records = (count.records.get * count.perColumn.get.count.get).toString
        val columns = count.perColumn.get.columnNames.mkString(",")
        val str = s"per-column-count: columns=$columns, num-records=$records"
        val list = List(
          List(COUNT_TYPE, COUNT_PER_COLUMN),
          List(COUNT_COLUMNS, columns),
          List(COUNT_NUM_RECORDS, records)
        )
        (str, list)
      } else if (count.perColumn.isDefined && count.perColumn.get.generator.isDefined) {
        val records = (count.records.get * count.perColumn.get.count.get).toString
        val columns = count.perColumn.get.columnNames.mkString(",")
        val str = s"per-column-count: columns=$columns, num-records-via-generator=$records"
        val list = List(
          List(COUNT_TYPE, COUNT_GENERATED_PER_COLUMN),
          List(COUNT_COLUMNS, columns),
          List(COUNT_NUM_RECORDS, records)
        )
        (str, list)
      } else if (count.records.isDefined) {
        val records = count.records.get.toString
        val str = s"basic-count: num-records=$records"
        val list = List(
          List(COUNT_TYPE, COUNT_BASIC),
          List(COUNT_NUM_RECORDS, records)
        )
        (str, list)
      } else if (count.generator.isDefined) {
        val records = count.generator.toString
        val str = s"generated-count: num-records=$records"
        val list = List(
          List(COUNT_TYPE, COUNT_GENERATED),
          List(COUNT_NUM_RECORDS, records)
        )
        (str, list)
      } else {
        //TODO: should throw error here?
        ("0", List())
      }
    }

    def numRecords: Long = {
      (count.records, count.generator, count.perColumn, count.perColumn.flatMap(_.generator)) match {
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
