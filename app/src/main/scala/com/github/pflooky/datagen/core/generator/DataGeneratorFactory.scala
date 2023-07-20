package com.github.pflooky.datagen.core.generator

import com.github.pflooky.datagen.core.exception.{InvalidCountGeneratorConfigurationException, InvalidFieldConfigurationException, InvalidStepCountGeneratorConfigurationException, UnsupportedDataGeneratorType}
import com.github.pflooky.datagen.core.generator.provider.{DataGenerator, OneOfDataGenerator, RandomDataGenerator, RegexDataGenerator}
import com.github.pflooky.datagen.core.model.Constants._
import com.github.pflooky.datagen.core.model._
import com.github.pflooky.datagen.core.util.GeneratorUtil.{getDataGenerator, getRecordCount}
import com.github.pflooky.datagen.core.util.ObjectMapperUtil
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession}

import java.io.Serializable
import java.util.{Locale, Random}
import scala.util.{Failure, Success, Try}

case class Holder(i: Int)

class DataGeneratorFactory(faker: Faker)(implicit val sparkSession: SparkSession) {

  private val OBJECT_MAPPER = ObjectMapperUtil.jsonObjectMapper
  sparkSession.udf.register("GENERATE_REGEX", udf((s: String) => faker.regexify(s)))
  sparkSession.udf.register("GENERATE_FAKER_EXPRESSION", udf((s: String) => faker.expression(s)))

  def generateDataForStep(step: Step, dataSourceName: String): DataFrame = {
    val structFieldsWithDataGenerators = if (step.schema.fields.isDefined) {
      getStructWithGenerators(step.schema.fields.get)
    } else {
      List()
    }

    generateDataViaSql(structFieldsWithDataGenerators, step)
      .alias(s"$dataSourceName.${step.name}")
  }

  def generateDataViaSql(dataGenerators: List[DataGenerator[_]], step: Step): DataFrame = {
    val structType = StructType(dataGenerators.map(_.structField))
    val recordCount = getRecordCount(step.count, faker).toInt

    val genSqlExpression = dataGenerators.filter(dg => !dg.structField.metadata.contains(SQL))
      .map(dg => s"${dg.generateSqlExpressionWrapper} AS `${dg.structField.name}`")
    val df = sparkSession.createDataFrame(Seq.fill(recordCount)(1).map(Holder))
      .selectExpr(genSqlExpression: _*)
    val sqlGeneratedFields = structType.fields.filter(f => f.metadata.contains(SQL))
      .map(f => s"${f.metadata.getString(SQL)} AS `${f.name}`")
    val dfAllFields = df.selectExpr(df.columns ++ sqlGeneratedFields: _*)
    if (step.count.perColumn.isDefined) {
      generateRecordsPerColumn(dataGenerators, step, step.count.perColumn.get, dfAllFields)
    } else {
      dfAllFields
    }
  }

  def generateData(dataGenerators: List[DataGenerator[_]], step: Step): DataFrame = {
    val structType = StructType(dataGenerators.map(_.structField))
    val count = step.count

    val generatedData = if (count.generator.isDefined) {
      val metadata = Metadata.fromJson(OBJECT_MAPPER.writeValueAsString(count.generator.get.options))
      val countStructField = StructField(RECORD_COUNT_GENERATOR_COL, IntegerType, false, metadata)
      val generatedCount = getDataGenerator(count.generator, countStructField, faker).generate.asInstanceOf[Int].toLong
      (1L to generatedCount).map(_ => Row.fromSeq(dataGenerators.map(_.generateWrapper())))
    } else if (count.total.isDefined) {
      (1L to count.total.get.asInstanceOf[Number].longValue()).map(_ => Row.fromSeq(dataGenerators.map(_.generateWrapper())))
    } else {
      throw new InvalidStepCountGeneratorConfigurationException(step)
    }

    val rddGeneratedData = sparkSession.sparkContext.parallelize(generatedData)
    val df = sparkSession.createDataFrame(rddGeneratedData, structType)
    df.cache()

    var dfPerCol = if (count.perColumn.isDefined) {
      generateRecordsPerColumn(dataGenerators, step, count.perColumn.get, df)
    } else {
      df
    }
    val sqlGeneratedFields = structType.fields.filter(f => f.metadata.contains(SQL))
    sqlGeneratedFields.foreach(field => {
      val allFields = structType.fields.filter(_ != field).map(_.name) ++ Array(s"${field.metadata.getString(SQL)} AS ${field.name}")
      dfPerCol = dfPerCol.selectExpr(allFields: _*)
    })
    dfPerCol
  }

  private def generateRecordsPerColumn(dataGenerators: List[DataGenerator[_]], step: Step,
                                       perColumnCount: PerColumnCount, df: DataFrame): DataFrame = {
    val fieldsToBeGenerated = dataGenerators.filter(x => !perColumnCount.columnNames.contains(x.structField.name))

    val perColumnRange = if (perColumnCount.generator.isDefined) {
      val metadata = Metadata.fromJson(OBJECT_MAPPER.writeValueAsString(perColumnCount.generator.get.options))
      val countStructField = StructField(RECORD_COUNT_GENERATOR_COL, IntegerType, false, metadata)
      val generatedCount = getDataGenerator(perColumnCount.generator, countStructField, faker).asInstanceOf[DataGenerator[Int]]
      val numList = generateDataWithSchema(generatedCount, fieldsToBeGenerated)
      df.withColumn(PER_COLUMN_COUNT, numList())
    } else if (perColumnCount.count.isDefined) {
      val numList = generateDataWithSchema(perColumnCount.count.get, fieldsToBeGenerated)
      df.withColumn(PER_COLUMN_COUNT, numList())
    } else {
      throw new InvalidStepCountGeneratorConfigurationException(step)
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
    val structFieldsWithDataGenerators = fields
      .filter(field => !field.generator.flatMap(gen => gen.options.get(OMIT)).getOrElse("false").toString.toBoolean)
      .map(field => {
        val structField: StructField = createStructFieldFromField(field)
        getDataGenerator(field.generator, structField, faker)
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

}
