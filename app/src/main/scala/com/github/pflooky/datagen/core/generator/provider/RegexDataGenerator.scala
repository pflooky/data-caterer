package com.github.pflooky.datagen.core.generator.provider

import com.github.pflooky.datagen.core.model.Constants.REGEX
import net.datafaker.Faker
import org.apache.spark.sql.types.StructField

object RegexDataGenerator {

  def getGenerator(structField: StructField, faker: Faker): DataGenerator[_] = {
    new RandomRegexDataGenerator(structField, faker)
  }

  class RandomRegexDataGenerator(val structField: StructField, val faker: Faker) extends NullableDataGenerator[String] {
    private val regex = structField.metadata.getString(REGEX)

    override val edgeCases: List[String] = List()

    override def generate: String = {
      faker.regexify(regex)
    }
  }

}
