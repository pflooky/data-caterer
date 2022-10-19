package com.github.pflooky.datagen.core.generator.provider

import com.github.curiousoddman.rgxgen.RgxGen

object RegexDataGenerator {

  def getGenerator(options: Map[String, Any], isNullable: Boolean): DataGenerator[_] = {
    new RandomRegexDataGenerator(options, isNullable)
  }

  class RandomRegexDataGenerator(val options: Map[String, Any], val isNullable: Boolean) extends NullableDataGenerator[String] {
    private val regex = new RgxGen(options("regex").toString)

    override def generate: String = {
      regex.generate(random.self)
    }
  }

}
