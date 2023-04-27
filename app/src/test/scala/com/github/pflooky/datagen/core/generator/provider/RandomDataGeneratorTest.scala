package com.github.pflooky.datagen.core.generator.provider

import com.github.pflooky.datagen.core.generator.provider.RandomDataGenerator._
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.sql.{Date, Timestamp}
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate}

@RunWith(classOf[JUnitRunner])
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
    val longGenerator = RandomDataGenerator.getGeneratorForStructField(StructField("field", LongType))
    val decimalGenerator = RandomDataGenerator.getGeneratorForStructField(StructField("field", DecimalType(20, 2)))
    val shortGenerator = RandomDataGenerator.getGeneratorForStructField(StructField("field", ShortType))
    val doubleGenerator = RandomDataGenerator.getGeneratorForStructField(StructField("field", DoubleType))
    val floatGenerator = RandomDataGenerator.getGeneratorForStructField(StructField("field", FloatType))
    val dateGenerator = RandomDataGenerator.getGeneratorForStructField(StructField("field", DateType))
    val timestampGenerator = RandomDataGenerator.getGeneratorForStructField(StructField("field", TimestampType))
    val booleanGenerator = RandomDataGenerator.getGeneratorForStructField(StructField("field", BooleanType))
    val binaryGenerator = RandomDataGenerator.getGeneratorForStructField(StructField("field", BinaryType))
    val byteGenerator = RandomDataGenerator.getGeneratorForStructField(StructField("field", ByteType))

    assert(stringGenerator.isInstanceOf[RandomStringDataGenerator])
    assert(intGenerator.isInstanceOf[RandomIntDataGenerator])
    assert(longGenerator.isInstanceOf[RandomLongDataGenerator])
    assert(decimalGenerator.isInstanceOf[RandomDecimalDataGenerator])
    assert(shortGenerator.isInstanceOf[RandomShortDataGenerator])
    assert(doubleGenerator.isInstanceOf[RandomDoubleDataGenerator])
    assert(floatGenerator.isInstanceOf[RandomFloatDataGenerator])
    assert(dateGenerator.isInstanceOf[RandomDateDataGenerator])
    assert(timestampGenerator.isInstanceOf[RandomTimestampDataGenerator])
    assert(booleanGenerator.isInstanceOf[RandomBooleanDataGenerator])
    assert(binaryGenerator.isInstanceOf[RandomBinaryDataGenerator])
    assert(byteGenerator.isInstanceOf[RandomByteDataGenerator])
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
    assert(sampleData <= Int.MaxValue)
  }

  test("Can create random long generator") {
    val longGenerator = new RandomLongDataGenerator(StructField("random_long", LongType, false))
    val sampleData = longGenerator.generate

    assert(longGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 0)
    assert(sampleData <= Long.MaxValue)
  }

  test("Can create random decimal generator") {
    val decimalGenerator = new RandomDecimalDataGenerator(StructField("random_decimal", DecimalType(22, 2), false))
    val sampleData = decimalGenerator.generate

    assert(decimalGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 0)
    assert(sampleData <= Long.MaxValue)
  }

  test("Can create random short generator") {
    val shortGenerator = new RandomShortDataGenerator(StructField("random_short", ShortType, false))
    val sampleData = shortGenerator.generate

    assert(shortGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 0)
    assert(sampleData <= Short.MaxValue)
  }

  test("Can create random double generator") {
    val doubleGenerator = new RandomDoubleDataGenerator(StructField("random_double", DoubleType, false))
    val sampleData = doubleGenerator.generate

    assert(doubleGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 0.0)
    assert(sampleData <= Double.MaxValue)
  }

  test("Can create random float generator") {
    val floatGenerator = new RandomFloatDataGenerator(StructField("random_float", FloatType, false))
    val sampleData = floatGenerator.generate

    assert(floatGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 0.0)
    assert(sampleData <= Float.MaxValue)
  }

  test("Can create random date generator") {
    val dateGenerator = new RandomDateDataGenerator(StructField("random_date", DateType, false))
    val sampleData = dateGenerator.generate

    assert(dateGenerator.edgeCases.nonEmpty)
    assert(sampleData.before(Date.valueOf(LocalDate.now())))
    assert(sampleData.after(Date.valueOf(LocalDate.now().minusYears(1))))
  }

  test("Can create random timestamp generator") {
    val dateGenerator = new RandomTimestampDataGenerator(StructField("random_ts", TimestampType, false))
    val sampleData = dateGenerator.generate

    assert(dateGenerator.edgeCases.nonEmpty)
    assert(sampleData.before(Timestamp.from(Instant.now())))
    assert(sampleData.after(Timestamp.from(Instant.now().minus(365, ChronoUnit.DAYS))))
  }

  test("Can create random binary generator") {
    val binaryGenerator = new RandomBinaryDataGenerator(StructField("random_binary", BinaryType, false))
    val sampleData = binaryGenerator.generate

    assert(binaryGenerator.edgeCases.nonEmpty)
    assert(sampleData.length > 0)
    assert(sampleData.length <= 20)
  }

  test("Can create random byte generator") {
    val byteGenerator = new RandomByteDataGenerator(StructField("random_byte", ByteType, false))
    val sampleData = byteGenerator.generate

    assert(byteGenerator.edgeCases.nonEmpty)
  }
}
