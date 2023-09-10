package com.github.pflooky.datagen.core.generator.provider

import com.github.pflooky.datacaterer.api.model.Constants.{ARRAY_MINIMUM_LENGTH, ENABLED_EDGE_CASE, ENABLED_NULL, EXPRESSION, IS_UNIQUE, MAXIMUM, MINIMUM, PROBABILITY_OF_EDGE_CASE, PROBABILITY_OF_NULL}
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
    val listGenerator = RandomDataGenerator.getGeneratorForStructField(StructField("field", ArrayType(StringType)))

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
    assert(listGenerator.isInstanceOf[RandomListDataGenerator[String]])
  }

  test("Can create random string generator") {
    val stringGenerator = new RandomStringDataGenerator(StructField("random_string", StringType, false))
    val sampleData = stringGenerator.generate

    assert(stringGenerator.edgeCases.nonEmpty)
    assert(sampleData.nonEmpty)
    assert(sampleData.length <= 20)
  }

  test("Can create random string generator with expression that ignores minimum and maximum length") {
    val metadata = new MetadataBuilder().putString(MINIMUM, "0").putString(MAXIMUM, "5").putString(EXPRESSION, "#{Name.name}").build()
    val stringGenerator = new RandomStringDataGenerator(StructField("random_string", StringType, false, metadata))
    val sampleData = stringGenerator.generate

    assert(stringGenerator.edgeCases.nonEmpty)
    assert(sampleData.nonEmpty)
    assert(sampleData.length > 5)
  }

  test("Can create random int generator with custom min and max") {
    val metadata = new MetadataBuilder().putString(MAXIMUM, "10").putString(MINIMUM, "5").build()
    val intGenerator = new RandomIntDataGenerator(StructField("random_int", IntegerType, false, metadata))
    val sampleData = intGenerator.generate

    assert(intGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 5)
    assert(sampleData <= 10)
  }

  ignore("Can create random int generator with metadata where minValue and maxValue take precedence over min and max") {
    val metadata = new MetadataBuilder().putString(MAXIMUM, "10").putString(MINIMUM, "5").build()
    val intGenerator = new RandomIntDataGenerator(StructField("random_int", IntegerType, false, metadata))
    val sampleData = intGenerator.generate

    assert(intGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 90)
    assert(sampleData <= 100)
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

  test("Can create random long generator with custom min and max") {
    val metadata = new MetadataBuilder().putString(MAXIMUM, "10").putString(MINIMUM, "5").build()
    val longGenerator = new RandomLongDataGenerator(StructField("random_long", LongType, false, metadata))
    val sampleData = longGenerator.generate

    assert(longGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 5)
    assert(sampleData <= 10)
  }

  test("Can create random decimal generator") {
    val decimalGenerator = new RandomDecimalDataGenerator(StructField("random_decimal", DecimalType(22, 2), false))
    val sampleData = decimalGenerator.generate

    assert(decimalGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 0)
    assert(sampleData <= Long.MaxValue)
  }

  test("Can create random decimal generator with custom min and max") {
    val metadata = new MetadataBuilder().putString(MAXIMUM, "10").putString(MINIMUM, "5").build()
    val decimalGenerator = new RandomDecimalDataGenerator(StructField("random_decimal", DecimalType(22, 2), false, metadata))
    val sampleData = decimalGenerator.generate

    assert(decimalGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 5)
    assert(sampleData <= 10)
  }

  test("Can create random short generator") {
    val shortGenerator = new RandomShortDataGenerator(StructField("random_short", ShortType, false))
    val sampleData = shortGenerator.generate

    assert(shortGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 0)
    assert(sampleData <= Short.MaxValue)
  }

  test("Can create random short generator with custom min and max") {
    val metadata = new MetadataBuilder().putString(MAXIMUM, "10").putString(MINIMUM, "5").build()
    val shortGenerator = new RandomShortDataGenerator(StructField("random_short", ShortType, false, metadata))
    val sampleData = shortGenerator.generate

    assert(shortGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 5)
    assert(sampleData <= 10)
  }

  test("Can create random double generator") {
    val doubleGenerator = new RandomDoubleDataGenerator(StructField("random_double", DoubleType, false))
    val sampleData = doubleGenerator.generate

    assert(doubleGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 0.0)
    assert(sampleData <= Double.MaxValue)
  }

  test("Can create random double generator with custom min and max") {
    val metadata = new MetadataBuilder().putString(MAXIMUM, "10.0").putString(MINIMUM, "5.0").build()
    val doubleGenerator = new RandomDoubleDataGenerator(StructField("random_double", DoubleType, false, metadata))
    val sampleData = doubleGenerator.generate

    assert(doubleGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 5.0)
    assert(sampleData <= 10.0)
  }

  test("Can create random float generator") {
    val floatGenerator = new RandomFloatDataGenerator(StructField("random_float", FloatType, false))
    val sampleData = floatGenerator.generate

    assert(floatGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 0.0)
    assert(sampleData <= Float.MaxValue)
  }

  test("Can create random float generator with custom min and max") {
    val metadata = new MetadataBuilder().putString(MAXIMUM, "10.0").putString(MINIMUM, "5.0").build()
    val floatGenerator = new RandomFloatDataGenerator(StructField("random_float", FloatType, false, metadata))
    val sampleData = floatGenerator.generate

    assert(floatGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 5.0)
    assert(sampleData <= 10.0)
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
    assert(sampleData.toString.nonEmpty)
  }

  test("Can create random list of string generator") {
    val metadata = new MetadataBuilder().putString(ARRAY_MINIMUM_LENGTH, "1").build()
    val listGenerator = new RandomListDataGenerator[String](StructField("random_list", ArrayType(StringType), false, metadata), StringType)
    val sampleData = listGenerator.generate

    assert(sampleData.nonEmpty)
  }

  test("Can create random list of struct type generator") {
    val metadata = new MetadataBuilder().putString(ARRAY_MINIMUM_LENGTH, "1").build()
    val innerStruct = StructType(Seq(StructField("random_acc", StringType), StructField("random_num", IntegerType)))
    val listGenerator = new RandomListDataGenerator[StructType](StructField("random_list", ArrayType(innerStruct), false, metadata), new StructType())
    val sampleData = listGenerator.generate

    assert(sampleData.nonEmpty)
  }

  test("Can create random string generator with only nulls generated") {
    val metadata = new MetadataBuilder().putString(ENABLED_NULL, "true").putString(PROBABILITY_OF_NULL, "1.0").build()
    val stringGenerator = new RandomStringDataGenerator(StructField("random_string", StringType, true, metadata))
    val sampleData = (1 to 10).map(_ => stringGenerator.generateWrapper())

    assert(stringGenerator.edgeCases.nonEmpty)
    assert(sampleData.nonEmpty)
    assert(sampleData.forall(_ == null))
  }

  test("Can create random string generator with only edge cases generated") {
    val metadata = new MetadataBuilder().putString(ENABLED_EDGE_CASE, "true").putString(PROBABILITY_OF_EDGE_CASE, "1.0").build()
    val stringGenerator = new RandomStringDataGenerator(StructField("random_string", StringType, false, metadata))
    val sampleData = (1 to 10).map(_ => stringGenerator.generateWrapper())

    assert(stringGenerator.edgeCases.nonEmpty)
    assert(sampleData.nonEmpty)
    assert(sampleData.forall(stringGenerator.edgeCases.contains))
  }

  test("Can create random int generator with only edge cases generated") {
    val metadata = new MetadataBuilder().putString(ENABLED_EDGE_CASE, "true").putString(PROBABILITY_OF_EDGE_CASE, "1.0").build()
    val intGenerator = new RandomIntDataGenerator(StructField("random_int", IntegerType, false, metadata))
    val sampleData = (1 to 10).map(_ => intGenerator.generateWrapper())

    assert(intGenerator.edgeCases.nonEmpty)
    assert(sampleData.nonEmpty)
    assert(sampleData.forall(intGenerator.edgeCases.contains))
  }

  test("Can create random string generator with nulls and edge cases enabled") {
    val metadata = new MetadataBuilder().putString(ENABLED_NULL, "true").putString(ENABLED_EDGE_CASE, "true")
      .putString(PROBABILITY_OF_EDGE_CASE, "1.0").build()
    val stringGenerator = new RandomStringDataGenerator(StructField("random_string", StringType, true, metadata))
    val sampleData = (1 to 10).map(_ => stringGenerator.generateWrapper())

    assert(stringGenerator.edgeCases.nonEmpty)
    assert(sampleData.nonEmpty)
    assert(sampleData.forall(s => stringGenerator.edgeCases.contains(s) || s == null))
  }

  test("Will throw exception if unable to create unique value given metadata restrictions") {
    val metadata = new MetadataBuilder().putString(MINIMUM, "1").putString(MAXIMUM, "1").putString(IS_UNIQUE, "true").build()
    val intGenerator = new RandomIntDataGenerator(StructField("random_int", IntegerType, false, metadata))
    intGenerator.generateWrapper()
    assertThrows[RuntimeException](intGenerator.generateWrapper())
  }
}
