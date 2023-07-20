package com.github.pflooky.datagen.core.util

import com.github.pflooky.datagen.core.exception.{InvalidCountGeneratorConfigurationException, UnsupportedDataGeneratorType}
import com.github.pflooky.datagen.core.generator.provider.{DataGenerator, OneOfDataGenerator, RandomDataGenerator, RegexDataGenerator}
import com.github.pflooky.datagen.core.model.Constants.{ONE_OF, RANDOM, RECORD_COUNT_GENERATOR_COL, REGEX, SQL}
import com.github.pflooky.datagen.core.model.{Count, Generator, Step}
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{LongType, Metadata, StructField}

object GeneratorUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def getDataGenerator(optGenerator: Option[Generator], structField: StructField, faker: Faker): DataGenerator[_] = {
    if (optGenerator.isDefined) {
      optGenerator.get.`type` match {
        //TODO: Slightly abusing random data generator giving back correct data type for sql type generated data
        case RANDOM | SQL => RandomDataGenerator.getGeneratorForStructField(structField, faker)
        case ONE_OF => OneOfDataGenerator.getGenerator(structField, faker)
        case REGEX => RegexDataGenerator.getGenerator(structField, faker)
        case x => throw new UnsupportedDataGeneratorType(x)
      }
    } else {
      LOGGER.debug(s"No generator defined, will default to random generator, field-name=${structField.name}")
      RandomDataGenerator.getGeneratorForStructField(structField, faker)
    }
  }

  def getRecordCount(count: Count, faker: Faker): Long = {
    (count.generator, count.total, count.perColumn) match {
      case (Some(generator), _, _) =>
        getGeneratedCount(generator, faker)
      case (_, Some(total), _) =>
        total.asInstanceOf[Number].longValue()
      case (_, _, Some(perColumnCount)) =>
        (perColumnCount.generator, perColumnCount.count) match {
          case (Some(generator), _) =>
            getGeneratedCount(generator, faker)
          case (_, Some(total)) =>
            total.asInstanceOf[Number].longValue()
          case _ => throw new InvalidCountGeneratorConfigurationException(count)
        }
      case _ => throw new InvalidCountGeneratorConfigurationException(count)
    }
  }

  private def getGeneratedCount(generator: Generator, faker: Faker): Long = {
    val metadata = Metadata.fromJson(ObjectMapperUtil.jsonObjectMapper.writeValueAsString(generator.options))
    val countStructField = StructField(RECORD_COUNT_GENERATOR_COL, LongType, false, metadata)
    getDataGenerator(Some(generator), countStructField, faker).generate.asInstanceOf[Long]
  }

}
