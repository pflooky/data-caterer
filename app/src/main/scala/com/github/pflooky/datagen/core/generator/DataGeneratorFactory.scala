package com.github.pflooky.datagen.core.generator

import com.github.pflooky.datagen.core.exception.{InvalidCountGeneratorConfigurationException, InvalidFieldConfigurationException, UnsupportedDataGeneratorType}
import com.github.pflooky.datagen.core.generator.provider.{DataGenerator, OneOfDataGenerator, RandomDataGenerator, RegexDataGenerator}
import com.github.pflooky.datagen.core.model.Constants._
import com.github.pflooky.datagen.core.model._
import com.github.pflooky.datagen.core.util.ObjectMapperUtil
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.Serializable
import java.util.{Locale, Random}
import scala.util.{Failure, Success, Try}

class DataGeneratorFactory(optSeed: Option[String], optLocale: Option[String])(implicit val sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val FAKER = getDataFaker
  private val OBJECT_MAPPER = ObjectMapperUtil.jsonObjectMapper

  def generateDataForStep(step: Step, dataSourceName: String): DataFrame = {
    val structFieldsWithDataGenerators = if (step.schema.fields.isDefined) {
      getStructWithGenerators(step.schema.fields.get)
    } else {
      List()
    }

    generateData(structFieldsWithDataGenerators, step)
      .alias(s"$dataSourceName.${step.name}")
  }

  def generateData(dataGenerators: List[DataGenerator[_]], step: Step): DataFrame = {
    val structType = StructType(dataGenerators.map(_.structField))
    val count = step.count

    val generatedData = if (count.generator.isDefined) {
      val metadata = Metadata.fromJson(OBJECT_MAPPER.writeValueAsString(count.generator.get.options))
      val countStructField = StructField(RECORD_COUNT_GENERATOR_COL, IntegerType, false, metadata)
      val generatedCount = getDataGenerator(count.generator, countStructField).generate.asInstanceOf[Int].toLong
      (1L to generatedCount).map(_ => Row.fromSeq(dataGenerators.map(_.generateWrapper())))
    } else if (count.total.isDefined) {
      (1L to count.total.get.asInstanceOf[Number].longValue()).map(_ => Row.fromSeq(dataGenerators.map(_.generateWrapper())))
    } else {
      throw new InvalidCountGeneratorConfigurationException(step)
    }

    val rddGeneratedData = sparkSession.sparkContext.parallelize(generatedData)
    val df = sparkSession.createDataFrame(rddGeneratedData, structType)
    df.cache()

    if (count.perColumn.isDefined) {
      generateRecordsPerColumn(dataGenerators, step, count.perColumn.get, df)
    } else {
      df
    }
  }

  private def generateRecordsPerColumn(dataGenerators: List[DataGenerator[_]], step: Step,
                                       perColumnCount: PerColumnCount, df: DataFrame): DataFrame = {
    val fieldsToBeGenerated = dataGenerators.filter(x => !perColumnCount.columnNames.contains(x.structField.name))

    val perColumnRange = if (perColumnCount.generator.isDefined) {
      val metadata = Metadata.fromJson(OBJECT_MAPPER.writeValueAsString(perColumnCount.generator.get.options))
      val countStructField = StructField(RECORD_COUNT_GENERATOR_COL, IntegerType, false, metadata)
      val generatedCount = getDataGenerator(perColumnCount.generator, countStructField).asInstanceOf[DataGenerator[Int]]
      val numList = generateDataWithSchema(generatedCount, fieldsToBeGenerated)
      df.withColumn(PER_COLUMN_COUNT, numList())
    } else if (perColumnCount.count.isDefined) {
      val numList = generateDataWithSchema(perColumnCount.count.get, fieldsToBeGenerated)
      df.withColumn(PER_COLUMN_COUNT, numList())
    } else {
      throw new InvalidCountGeneratorConfigurationException(step)
    }

    val explodeCount = perColumnRange.withColumn(PER_COLUMN_INDEX_COL, explode(col(PER_COLUMN_COUNT)))
      .drop(col(PER_COLUMN_COUNT))
    explodeCount.select(PER_COLUMN_INDEX_COL + ".*", perColumnCount.columnNames: _*)
  }

  private def generateDataWithSchema(countGenerator: DataGenerator[Int], dataGenerators: List[DataGenerator[_]]): UserDefinedFunction = {
    udf(() => {
      (1L to countGenerator.generate)
        .toList
        .map(_ => Row.fromSeq(dataGenerators.map(_.generateWrapper())))
    }, ArrayType(StructType(dataGenerators.map(_.structField))))
  }

  private def generateDataWithSchema(count: Long, dataGenerators: List[DataGenerator[_]]): UserDefinedFunction = {
    udf(() => {
      (1L to count)
        .toList
        .map(_ => Row.fromSeq(dataGenerators.map(_.generateWrapper())))
    }, ArrayType(StructType(dataGenerators.map(_.structField))))
  }

  private def getStructWithGenerators(fields: List[Field]): List[DataGenerator[_]] = {
    val structFieldsWithDataGenerators = fields.map(field => {
      val structField: StructField = createStructFieldFromField(field)
      getDataGenerator(field.generator, structField)
    })
    structFieldsWithDataGenerators
  }

  private def createStructFieldFromField(field: Field): StructField = {
    if (field.schema.isDefined) {
      val innerStructFields = createStructTypeFromSchema(field.schema.get)
      if (field.`type`.isDefined && field.`type`.get.toLowerCase == "array") {
        StructField(field.name, ArrayType(innerStructFields, field.nullable), field.nullable)
      } else {
        StructField(field.name, innerStructFields, field.nullable)
      }
    } else if (field.generator.isDefined && field.`type`.isDefined) {
      val metadata = Metadata.fromJson(OBJECT_MAPPER.writeValueAsString(field.generator.get.options))
      StructField(field.name, DataType.fromDDL(field.`type`.get), field.nullable, metadata)
    } else {
      throw new InvalidFieldConfigurationException(field)
    }
  }

  private def createStructTypeFromSchema(schema: Schema): StructType = {
    if (schema.fields.isDefined) {
      val structFields = schema.fields.get.map(createStructFieldFromField)
      StructType(structFields)
    } else {
      StructType(Seq())
    }
  }

  private def getDataGenerator(optGenerator: Option[Generator], structField: StructField): DataGenerator[_] = {
    if (optGenerator.isDefined) {
      optGenerator.get.`type` match {
        case RANDOM => RandomDataGenerator.getGeneratorForStructField(structField, FAKER)
        case ONE_OF => OneOfDataGenerator.getGenerator(structField, FAKER)
        case REGEX => RegexDataGenerator.getGenerator(structField, FAKER)
        case x => throw new UnsupportedDataGeneratorType(x)
      }
    } else {
      LOGGER.debug(s"No generator defined, will default to random generator, field-name=${structField.name}")
      RandomDataGenerator.getGeneratorForStructField(structField, FAKER)
    }
  }

  private def getDataFaker: Faker with Serializable = {
    val trySeed = Try(optSeed.map(_.toInt).get)
    (optSeed, trySeed, optLocale) match {
      case (None, _, Some(locale)) =>
        LOGGER.info(s"Locale defined at plan level. All data will be generated with the set locale, locale=$locale")
        new Faker(Locale.forLanguageTag(locale)) with Serializable
      case (Some(_), Success(seed), Some(locale)) =>
        LOGGER.info(s"Seed and locale defined at plan level. All data will be generated with the set seed and locale, seed-value=$seed, locale=$locale")
        new Faker(Locale.forLanguageTag(locale), new Random(seed)) with Serializable
      case (Some(_), Failure(exception), _) =>
        throw new RuntimeException(s"Failed to get seed value from plan sink options", exception)
      case _ => new Faker() with Serializable
    }
  }
}
