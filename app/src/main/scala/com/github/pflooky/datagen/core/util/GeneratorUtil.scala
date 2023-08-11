package com.github.pflooky.datagen.core.util

import com.github.pflooky.datagen.core.exception.{InvalidCountGeneratorConfigurationException, UnsupportedDataGeneratorType}
import com.github.pflooky.datagen.core.generator.provider.{DataGenerator, OneOfDataGenerator, RandomDataGenerator, RegexDataGenerator}
import com.github.pflooky.datagen.core.model.Constants.{ONE_OF_GENERATOR, RANDOM_GENERATOR, RECORD_COUNT_GENERATOR_COL, REGEX_GENERATOR, SQL_GENERATOR}
import com.github.pflooky.datagen.core.model.{Count, Generator, Step, TaskSummary}
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{LongType, Metadata, StructField, StructType}

object GeneratorUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def getDataGenerator(optGenerator: Option[Generator], structField: StructField, faker: Faker): DataGenerator[_] = {
    if (optGenerator.isDefined) {
      optGenerator.get.`type` match {
        //TODO: Slightly abusing random data generator giving back correct data type for sql type generated data
        case RANDOM_GENERATOR | SQL_GENERATOR => RandomDataGenerator.getGeneratorForStructField(structField, faker)
        case ONE_OF_GENERATOR => OneOfDataGenerator.getGenerator(structField, faker)
        case REGEX_GENERATOR => RegexDataGenerator.getGenerator(structField, faker)
        case x => throw new UnsupportedDataGeneratorType(x)
      }
    } else {
      LOGGER.debug(s"No generator defined, will default to random generator, field-name=${structField.name}")
      RandomDataGenerator.getGeneratorForStructField(structField, faker)
    }
  }

  def zipWithIndex(df: DataFrame, colName: String): DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(ln =>
        Row.fromSeq(ln._1.toSeq ++ Seq(ln._2))
      ),
      StructType(
        df.schema.fields ++ Array(StructField(colName, LongType, false))
      )
    )
  }

  def getDataSourceName(taskSummary: TaskSummary, step: Step): String = {
    s"${taskSummary.dataSourceName}.${step.name}"
  }

  private def getGeneratedCount(generator: Generator, faker: Faker): Long = {
    val metadata = Metadata.fromJson(ObjectMapperUtil.jsonObjectMapper.writeValueAsString(generator.options))
    val countStructField = StructField(RECORD_COUNT_GENERATOR_COL, LongType, false, metadata)
    getDataGenerator(Some(generator), countStructField, faker).generate.asInstanceOf[Long]
  }

}
