package com.github.pflooky.datagen.core.generator.provider

import com.github.pflooky.datagen.core.exception.UnsupportedDataGeneratorType
import com.github.pflooky.datagen.core.generator.provider.RandomDataGenerator._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{Date, Timestamp}
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate}

class RandomDataGeneratorTest extends AnyFunSuite {

  test("Can get correct data generator based on StructType") {
    val structType = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("amount", DoubleType),
      StructField("date_of_birth", DateType),
      StructField("last_login_time", TimestampType)
    ))
    val generators = RandomDataGenerator.getGeneratorForStructType(structType)
    assert(generators.length == 5)
  }

  test("Can get the correct data generator based on return type") {
    val stringGenerator = RandomDataGenerator.getGeneratorForStructField(StructField("field", StringType))
    val intGenerator = RandomDataGenerator.getGeneratorForStructField(StructField("field", IntegerType))
    val doubleGenerator = RandomDataGenerator.getGeneratorForStructField(StructField("field", DoubleType))
    val dateGenerator = RandomDataGenerator.getGeneratorForStructField(StructField("field", DateType))
    val timestampGenerator = RandomDataGenerator.getGeneratorForStructField(StructField("field", TimestampType))
    val booleanGenerator = RandomDataGenerator.getGeneratorForStructField(StructField("field", BooleanType))

    assert(stringGenerator.isInstanceOf[RandomStringDataGenerator])
    assert(intGenerator.isInstanceOf[RandomIntDataGenerator])
    assert(doubleGenerator.isInstanceOf[RandomDoubleDataGenerator])
    assert(dateGenerator.isInstanceOf[RandomDateDataGenerator])
    assert(timestampGenerator.isInstanceOf[RandomTimestampDataGenerator])
    assert(booleanGenerator.isInstanceOf[RandomBooleanDataGenerator])
    assertThrows[UnsupportedDataGeneratorType](RandomDataGenerator.getGeneratorForStructField(StructField("field", ByteType)))
    assertThrows[UnsupportedDataGeneratorType](RandomDataGenerator.getGeneratorForStructField(StructField("field", FloatType)))
    assertThrows[UnsupportedDataGeneratorType](RandomDataGenerator.getGeneratorForStructField(StructField("field", LongType)))
    assertThrows[UnsupportedDataGeneratorType](RandomDataGenerator.getGeneratorForStructField(StructField("field", ShortType)))
  }

  test("Can create random string generator") {
    val stringGenerator = new RandomStringDataGenerator(StructField("random_string", StringType, false))
    val sampleData = stringGenerator.generate

    assert(stringGenerator.edgeCases.nonEmpty)
    assert(sampleData.nonEmpty)
    assert(sampleData.length <= 20)
  }

  test("Can create random int generator") {
    val intGenerator = new RandomIntDataGenerator(StructField("random_int", IntegerType, false))
    val sampleData = intGenerator.generate

    assert(intGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 0)
    assert(sampleData <= 1)
  }

  test("Can create random double generator") {
    val doubleGenerator = new RandomDoubleDataGenerator(StructField("random_double", DoubleType, false))
    val sampleData = doubleGenerator.generate

    assert(doubleGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 0.0)
    assert(sampleData <= 1.0)
  }

  test("Can create random date generator") {
    val dateGenerator = new RandomDateDataGenerator(StructField("random_date", DateType, false))
    val sampleData = dateGenerator.generate

    assert(dateGenerator.edgeCases.nonEmpty)
    assert(sampleData.before(Date.valueOf(LocalDate.now())))
    assert(sampleData.after(Date.valueOf(LocalDate.now().minusDays(6))))
  }

  test("Can create random timestamp generator") {
    val dateGenerator = new RandomTimestampDataGenerator(StructField("random_ts", TimestampType, false))
    val sampleData = dateGenerator.generate

    assert(dateGenerator.edgeCases.nonEmpty)
    assert(sampleData.before(Timestamp.from(Instant.now())))
    assert(sampleData.after(Timestamp.from(Instant.now().minus(6, ChronoUnit.DAYS))))
  }
}
