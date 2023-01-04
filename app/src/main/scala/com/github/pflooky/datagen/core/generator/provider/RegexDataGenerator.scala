package com.github.pflooky.datagen.core.generator.provider

import com.github.curiousoddman.rgxgen.RgxGen
import com.github.pflooky.datagen.core.model.Constants.REGEX
import org.apache.spark.sql.types.StructField

object RegexDataGenerator {

  def getGenerator(structField: StructField): DataGenerator[_] = {
    new RandomRegexDataGenerator(structField)
  }

  class RandomRegexDataGenerator(val structField: StructField) extends NullableDataGenerator[String] {
    private val regex = new RgxGen(structField.metadata.getString(REGEX))

    override val edgeCases: List[String] = List()

    override def generate: String = {
      regex.generate(random.self)
    }
  }

}
