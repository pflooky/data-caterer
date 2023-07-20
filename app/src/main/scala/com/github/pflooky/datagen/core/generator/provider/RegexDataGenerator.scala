package com.github.pflooky.datagen.core.generator.provider

import com.github.pflooky.datagen.core.exception.InvalidDataGeneratorConfigurationException
import com.github.pflooky.datagen.core.model.Constants.REGEX
import net.datafaker.Faker
import org.apache.spark.sql.types.StructField

import scala.util.Try

object RegexDataGenerator {

  def getGenerator(structField: StructField, faker: Faker = new Faker()): DataGenerator[_] = {
    new RandomRegexDataGenerator(structField, faker)
  }

  class RandomRegexDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends NullableDataGenerator[String] {
    private val regex = Try(structField.metadata.getString(REGEX))
      .getOrElse(throw new InvalidDataGeneratorConfigurationException(structField, REGEX))

    override val edgeCases: List[String] = List()

    override def generate: String = {
      faker.regexify(regex)
    }

    override def generateSqlExpression: String = {
      s"GENERATE_REGEX('$regex')"
    }
  }

}
