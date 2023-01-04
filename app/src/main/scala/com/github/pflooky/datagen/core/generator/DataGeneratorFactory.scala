package com.github.pflooky.datagen.core.generator

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.pflooky.datagen.core.exception.UnsupportedDataGeneratorType
import com.github.pflooky.datagen.core.generator.provider.{DataGenerator, OneOfDataGenerator, RandomDataGenerator, RegexDataGenerator}
import com.github.pflooky.datagen.core.model.Constants._
import com.github.pflooky.datagen.core.model._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class DataGeneratorFactory(implicit val sparkSession: SparkSession) {

  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)

  def generateDataForStep(step: Step, sinkName: String): DataFrame = {
    val structFieldsWithDataGenerators = if (step.schema.fields.isDefined) {
      getStructWithGenerators(step.schema.fields.get)
    } else {
      List()
    }

    //TODO: separate batch service to determine how to batch generate the data base on Count details
    //TODO: batch service should go through all the tasks per batch run
    generateData(structFieldsWithDataGenerators, step.count)
      .alias(s"${sinkName}.${step.name}")
  }

  def generateData(dataGenerators: List[DataGenerator[_]], count: Count): DataFrame = {
    val structType = StructType(dataGenerators.map(_.structField))

    val generatedData = if (count.generator.isDefined) {
      val metadata = Metadata.fromJson(objectMapper.writeValueAsString(count.generator.get.options))
      val countStructField = StructField(RECORD_COUNT_GENERATOR_COL, IntegerType, false, metadata)
      val generatedCount = getDataGenerator(count.generator.get, countStructField).generate.asInstanceOf[Int].toLong
      (1L to generatedCount).map(_ => Row.fromSeq(dataGenerators.map(_.generateWrapper)))
    } else if (count.total.isDefined) {
      (1L to count.total.get.asInstanceOf[Number].longValue()).map(_ => Row.fromSeq(dataGenerators.map(_.generateWrapper)))
    } else {
      throw new RuntimeException("Need to defined 'total' or 'generator' for generating rows")
    }

    val rddGeneratedData = sparkSession.sparkContext.parallelize(generatedData)
    val df = sparkSession.createDataFrame(rddGeneratedData, structType)
    df.cache()

    if (count.perColumn.isDefined) {
      generateRecordsPerColumn(dataGenerators, count.perColumn.get, df)
    } else {
      df
    }
  }

  private def generateRecordsPerColumn(dataGenerators: List[DataGenerator[_]],
                                       perColumnCount: PerColumnCount, df: DataFrame): DataFrame = {
    val fieldsToBeGenerated = dataGenerators.filter(x => !perColumnCount.columnNames.contains(x.structField.name))

    val perColumnRange = if (perColumnCount.generator.isDefined) {
      val metadata = Metadata.fromJson(objectMapper.writeValueAsString(perColumnCount.generator.get.options))
      val countStructField = StructField(RECORD_COUNT_GENERATOR_COL, IntegerType, false, metadata)
      val generatedCount = getDataGenerator(perColumnCount.generator.get, countStructField)
      val numList = generateDataWithSchema(generatedCount.generate.asInstanceOf[Int], fieldsToBeGenerated)
      df.withColumn(PER_COLUMN_COUNT, numList())
    } else if (perColumnCount.count.isDefined) {
      val numList = generateDataWithSchema(perColumnCount.count.get, fieldsToBeGenerated)
      df.withColumn(PER_COLUMN_COUNT, numList())
    } else {
      throw new RuntimeException("Need to defined 'total' or 'generator' for generating number of rows per column")
    }

    val explodeCount = perColumnRange.withColumn(PER_COLUMN_INDEX_COL, explode(col(PER_COLUMN_COUNT)))
      .drop(col(PER_COLUMN_COUNT))
    explodeCount.select(PER_COLUMN_INDEX_COL + ".*", perColumnCount.columnNames: _*)
  }

  private def generateDataWithSchema(count: Long, dataGenerators: List[DataGenerator[_]]): UserDefinedFunction = {
    udf(() => {
      (1L to count)
        .toList
        .map(_ => Row.fromSeq(dataGenerators.map(_.generateWrapper)))
    }, ArrayType(StructType(dataGenerators.map(_.structField))))
  }

  private def getStructWithGenerators(fields: List[Field]): List[DataGenerator[_]] = {
    val structFieldsWithDataGenerators = fields.map(field => {
      val structField: StructField = createStructFieldFromField(field)
      getDataGenerator(field.generator, structField)
    })
    structFieldsWithDataGenerators
  }

  private def createStructFieldFromField(field: Field) = {
    val metadata = Metadata.fromJson(objectMapper.writeValueAsString(field.generator.options))
    StructField(field.name, DataType.fromDDL(field.`type`), field.nullable, metadata)
  }

  private def getDataGenerator(generator: Generator, structField: StructField): DataGenerator[_] = {
    generator.`type` match {
      case RANDOM => RandomDataGenerator.getGeneratorForStructField(structField)
      case ONE_OF => OneOfDataGenerator.getGenerator(structField)
      case REGEX => RegexDataGenerator.getGenerator(structField)
      case x => throw new UnsupportedDataGeneratorType(x)
    }
  }
}
