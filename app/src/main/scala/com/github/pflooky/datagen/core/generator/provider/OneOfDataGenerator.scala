package com.github.pflooky.datagen.core.generator.provider

import com.github.pflooky.datagen.core.model.Constants._
import net.datafaker.Faker
import org.apache.spark.sql.types.StructField

import scala.util.Try

object OneOfDataGenerator {

  def getGenerator(structField: StructField, faker: Faker = new Faker()): DataGenerator[Any] = {
    new RandomOneOfDataGenerator(structField, faker)
  }

  class RandomOneOfDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Any] {
    private lazy val oneOfValues = getOneOfList
    private lazy val oneOfArrayLength = oneOfValues.length
    assert(structField.metadata.contains(ONE_OF), s"$ONE_OF not defined for data generator in metadata, unable to generate data, name=${structField.name}, " +
      s"type=${structField.dataType}, metadata=${structField.metadata}")

    override def generate: Any = {
      oneOfValues(random.nextInt(oneOfArrayLength))
    }

    override def generateSqlExpression: String = {
      val oneOfValuesString = oneOfValues.mkString("||")
      s"CAST(SPLIT('$oneOfValuesString', '\\\\|\\\\|')[CAST(RAND() * $oneOfArrayLength AS INT)] AS ${structField.dataType.sql})"
    }

    private def getOneOfList: Array[Any] = {
      lazy val arrayType = Try(structField.metadata.getString(ARRAY_TYPE)).getOrElse(ONE_OF_STRING)
      val x = arrayType.toLowerCase match {
        case ONE_OF_STRING => structField.metadata.getStringArray(ONE_OF)
        case ONE_OF_LONG => structField.metadata.getLongArray(ONE_OF)
        case ONE_OF_DOUBLE => structField.metadata.getDoubleArray(ONE_OF)
        case ONE_OF_BOOLEAN => structField.metadata.getBooleanArray(ONE_OF)
        case _ => Array()
      }
      x.asInstanceOf[Array[Any]]
    }
  }

}
