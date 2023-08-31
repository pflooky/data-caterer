package com.github.pflooky.datagen.core.generator.provider

import com.github.pflooky.datacaterer.api.model.Constants.ONE_OF_GENERATOR
import net.datafaker.Faker
import org.apache.spark.sql.types.StructField

object OneOfDataGenerator {

  def getGenerator(structField: StructField, faker: Faker = new Faker()): DataGenerator[Any] = {
    new RandomOneOfDataGenerator(structField, faker)
  }

  class RandomOneOfDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Any] {
    private lazy val oneOfValues = getOneOfList
    private lazy val oneOfArrayLength = oneOfValues.length
    assert(structField.metadata.contains(ONE_OF_GENERATOR), s"$ONE_OF_GENERATOR not defined for data generator in metadata, unable to generate data, name=${structField.name}, " +
      s"type=${structField.dataType}, metadata=${structField.metadata}")

    override def generate: Any = {
      oneOfValues(random.nextInt(oneOfArrayLength))
    }

    override def generateSqlExpression: String = {
      val oneOfValuesString = oneOfValues.mkString("||")
      s"CAST(SPLIT('$oneOfValuesString', '\\\\|\\\\|')[CAST(RAND() * $oneOfArrayLength AS INT)] AS ${structField.dataType.sql})"
    }

    private def getOneOfList: Array[String] = {
      structField.metadata.getStringArray(ONE_OF_GENERATOR)
    }
  }

}
