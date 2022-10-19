package com.github.pflooky.datagen.core.generator

import com.github.pflooky.datagen.core.generator.provider.{DataGenerator, OneOfDataGenerator, RandomDataGenerator, RegexDataGenerator}
import com.github.pflooky.datagen.core.model._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class DataGeneratorFactory(implicit val sparkSession: SparkSession) {

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

  def generateData(structFieldsWithDataGenerators: List[(StructField, DataGenerator[_])], count: Count): DataFrame = {
    val structType = StructType(structFieldsWithDataGenerators.map(_._1))
    val dataGenerators = structFieldsWithDataGenerators.map(_._2)

    val generatedData = if (count.generator.isDefined) {
      val generatedCount = getDataGenerator(count.generator.get, "int", false).asInstanceOf[Int]
      (1 to generatedCount).map(_ => Row.fromSeq(dataGenerators.map(_.generate)))
    } else {
      (1 to count.total.toInt).map(_ => Row.fromSeq(dataGenerators.map(_.generate)))
    }

    val rddGeneratedData = sparkSession.sparkContext.parallelize(generatedData)
    val df = sparkSession.createDataFrame(rddGeneratedData, structType)
    df.cache()

    if (count.perColumn.isDefined) {
      generateRecordsPerColumn(structFieldsWithDataGenerators, count.perColumn.get, df)
    } else {
      df
    }
  }

  private def generateRecordsPerColumn(structFieldsWithDataGenerators: List[(StructField, DataGenerator[_])],
                                       perColumnCount: PerColumnCount, df: DataFrame) = {
    val fieldsToBeGenerated = structFieldsWithDataGenerators.filter(x => !perColumnCount.columnNames.contains(x._1.name))
    val structForNewFields = fieldsToBeGenerated.map(_._1)
    val fieldsToBeGenDataGenerators = fieldsToBeGenerated.map(_._2)

    val perColumnRange = if (perColumnCount.generator.isDefined) {
      val generatedCount = getDataGenerator(perColumnCount.generator.get, "int", false)
      val numList = udf(() => {
        (1 to generatedCount.generate.asInstanceOf[Int])
          .toList
          .map(_ => Row.fromSeq(fieldsToBeGenDataGenerators.map(_.generate)))
      }, ArrayType(StructType(structForNewFields)))
      df.withColumn("_per_col_count", numList())
    } else {
      df.withColumn("_per_col_count", lit((1 to perColumnCount.count.toInt)
        .map(_ => Row(fieldsToBeGenDataGenerators.map(_.generate)))))
    }

    val explodeCount = perColumnRange.withColumn("_per_col_index", explode(col("_per_col_count")))
      .drop(col("_per_col_count"))
    explodeCount.select("_per_col_index.*", perColumnCount.columnNames: _*)
  }

  private def getStructWithGenerators(fields: List[Field]): List[(StructField, DataGenerator[_])] = {
    val structFieldsWithDataGenerators = fields.map(field => {
      val generator = getDataGenerator(field.generator, field.`type`, field.nullable)
      val structField = StructField(field.name, DataType.fromDDL(field.`type`), field.nullable)
      (structField, generator)
    })
    structFieldsWithDataGenerators
  }

  private def getDataGenerator(generator: Generator, returnType: String, isNullable: Boolean): DataGenerator[_] = {
    generator.`type` match {
      case "random" => RandomDataGenerator.getGenerator(returnType, generator.options, isNullable)
      case "oneOf" => OneOfDataGenerator.getGenerator(generator.options)
      case "regex" => RegexDataGenerator.getGenerator(generator.options, isNullable)
      case x => throw new RuntimeException(s"Unsupported generator type, type=$x")
    }
  }
}
