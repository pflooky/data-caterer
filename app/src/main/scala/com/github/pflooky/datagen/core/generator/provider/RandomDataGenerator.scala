package com.github.pflooky.datagen.core.generator.provider

import com.github.pflooky.datagen.core.exception.UnsupportedDataGeneratorType
import com.github.pflooky.datagen.core.model.Constants._
import net.datafaker.Faker
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Try

object RandomDataGenerator {

  def getGeneratorForStructType(structType: StructType, faker: Faker = new Faker()): Array[DataGenerator[_]] = {
    structType.fields.map(getGeneratorForStructField(_, faker))
  }

  def getGeneratorForStructField(structField: StructField, faker: Faker = new Faker()): DataGenerator[_] = {
    structField.dataType match {
      case StringType => new RandomStringDataGenerator(structField, faker)
      case IntegerType => new RandomIntDataGenerator(structField, faker)
      case LongType => new RandomLongDataGenerator(structField, faker)
      case ShortType => new RandomShortDataGenerator(structField, faker)
      case DecimalType() => new RandomDecimalDataGenerator(structField, faker)
      case DoubleType => new RandomDoubleDataGenerator(structField, faker)
      case FloatType => new RandomFloatDataGenerator(structField, faker)
      case DateType => new RandomDateDataGenerator(structField, faker)
      case TimestampType => new RandomTimestampDataGenerator(structField, faker)
      case BooleanType => new RandomBooleanDataGenerator(structField, faker)
      case BinaryType => new RandomBinaryDataGenerator(structField, faker)
      case x => throw new UnsupportedDataGeneratorType(s"Unsupported type for random data generation: name=${structField.name}, type=${x.typeName}")
    }
  }

  class RandomStringDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends NullableDataGenerator[String] {
    private lazy val minLength = Try(structField.metadata.getLong(MINIMUM_LENGTH)).getOrElse(1L).toInt
    private lazy val maxLength = Try(structField.metadata.getLong(MAXIMUM_LENGTH)).getOrElse(10L).toInt
    assert(minLength <= maxLength, s"minLength has to be less than or equal to maxLength, field-name${structField.name}")
    private lazy val tryExpression = Try(structField.metadata.getString(EXPRESSION))

    override val edgeCases: List[String] = List("", "\n", "\r", "\t", " ", "\\u0000", "\\ufff")

    override def generate: String = {
      if (tryExpression.isSuccess) {
        faker.expression(tryExpression.get)
      } else {
        val stringLength = (random.nextDouble() * (maxLength - minLength) + minLength).toInt
        random.alphanumeric.take(stringLength).mkString
      }
    }
  }

  class RandomIntDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Int] {
    private lazy val minValue = Try(structField.metadata.getString(MINIMUM_VALUE).toInt).getOrElse(0)
    private lazy val maxValue = Try(structField.metadata.getString(MAXIMUM_VALUE).toInt).getOrElse(Int.MaxValue - 1)
    assert(minValue <= maxValue, s"minValue has to be less than or equal to maxValue, field-name${structField.name}")

    override val edgeCases: List[Int] = List(Int.MaxValue, Int.MinValue, 0)

    override def generate: Int = {
      faker.random().nextInt(minValue, maxValue)
    }
  }

  class RandomShortDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Short] {
    private lazy val minValue = Try(structField.metadata.getString(MINIMUM_VALUE).toShort).getOrElse(0.toShort)
    private lazy val maxValue = Try(structField.metadata.getString(MAXIMUM_VALUE).toShort).getOrElse(Short.MaxValue)
    assert(minValue <= maxValue, s"minValue has to be less than or equal to maxValue, field-name${structField.name}")

    override val edgeCases: List[Short] = List(Short.MaxValue, Short.MinValue, 0)

    override def generate: Short = {
      (random.nextDouble() * (maxValue - minValue) + minValue).toShort
    }
  }

  class RandomLongDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Long] {
    private lazy val minValue = Try(structField.metadata.getString(MINIMUM_VALUE).toLong).getOrElse(0L)
    private lazy val maxValue = Try(structField.metadata.getString(MAXIMUM_VALUE).toLong).getOrElse(Long.MaxValue)
    assert(minValue <= maxValue, s"minValue has to be less than or equal to maxValue, field-name${structField.name}")

    override val edgeCases: List[Long] = List(Long.MaxValue, Long.MinValue, 0)

    override def generate: Long = {
      faker.random().nextLong(minValue, maxValue)
    }
  }

  class RandomDecimalDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[BigDecimal] {
    private lazy val minValue = Try(BigDecimal.valueOf(structField.metadata.getString(MINIMUM_VALUE).toLong)).getOrElse(BigDecimal.valueOf(0))
    private lazy val maxValue = Try(BigDecimal.valueOf(structField.metadata.getString(MAXIMUM_VALUE).toLong)).getOrElse(BigDecimal.valueOf(Long.MaxValue))
    assert(minValue <= maxValue, s"minValue has to be less than or equal to maxValue, field-name${structField.name}")

    override val edgeCases: List[BigDecimal] = List(BigDecimal.valueOf(Long.MaxValue), Long.MinValue, 0)

    override def generate: BigDecimal = {
      random.nextDouble() * (maxValue - minValue) + minValue
    }
  }

  class RandomDoubleDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Double] {
    private lazy val minValue = Try(structField.metadata.getString(MINIMUM_VALUE).toDouble).getOrElse(0.0)
    private lazy val maxValue = Try(structField.metadata.getString(MAXIMUM_VALUE).toDouble).getOrElse(Double.MaxValue)
    assert(minValue <= maxValue, s"minValue has to be less than or equal to maxValue, field-name${structField.name}")

    override val edgeCases: List[Double] = List(Double.PositiveInfinity, Double.MaxValue, Double.MinPositiveValue,
      0.0, -0.0, Double.MinValue, Double.NegativeInfinity, Double.NaN)

    override def generate: Double = {
      faker.random().nextDouble(minValue, maxValue)
    }
  }

  class RandomFloatDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Float] {
    private lazy val minValue = Try(structField.metadata.getString(MINIMUM_VALUE).toFloat).getOrElse(0.0.toFloat)
    private lazy val maxValue = Try(structField.metadata.getString(MAXIMUM_VALUE).toFloat).getOrElse(Float.MaxValue)
    assert(minValue <= maxValue, s"minValue has to be less than or equal to maxValue, field-name${structField.name}")

    override val edgeCases: List[Float] = List(Float.PositiveInfinity, Float.MaxValue, Float.MinPositiveValue,
      0.0f, -0.0f, Float.MinValue, Float.NegativeInfinity, Float.NaN)

    override def generate: Float = {
      faker.random().nextDouble(minValue, maxValue).toFloat
    }
  }

  class RandomDateDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends NullableDataGenerator[Date] {
    private lazy val minValue = getMinValue
    private lazy val maxValue = getMaxValue
    assert(minValue.isBefore(maxValue), s"minValue has to be less than or equal to maxValue, field-name${structField.name}")
    private lazy val maxDays = java.time.temporal.ChronoUnit.DAYS.between(minValue, maxValue).toInt

    //from here: https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/RandomDataGenerator.scala#L206
    override val edgeCases: List[Date] = List(
      Date.valueOf("0001-01-01"),
      Date.valueOf("1582-10-15"),
      Date.valueOf("1970-01-01"),
      Date.valueOf("9999-12-31")
    )

    override def generate: Date = {
      Date.valueOf(minValue.plusDays(random.nextInt(maxDays)))
    }

    private def getMinValue: LocalDate = {
      Try(structField.metadata.getString(MINIMUM_VALUE)).map(LocalDate.parse)
        .getOrElse(LocalDate.now().minusDays(365))
    }

    private def getMaxValue: LocalDate = {
      Try(structField.metadata.getString(MAXIMUM_VALUE)).map(LocalDate.parse)
        .getOrElse(LocalDate.now())
    }
  }

  class RandomTimestampDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends NullableDataGenerator[Timestamp] {
    private lazy val minValue = getMinValue
    private lazy val maxValue = getMaxValue
    assert(minValue <= maxValue, s"minValue has to be less than or equal to maxValue, field-name${structField.name}")

    //from here: https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/RandomDataGenerator.scala#L159
    override val edgeCases: List[Timestamp] = List(
      Timestamp.valueOf("0001-01-01 00:00:00"),
      Timestamp.valueOf("1582-10-15 23:59:59"),
      Timestamp.valueOf("1970-01-01 00:00:00"),
      Timestamp.valueOf("9999-12-31 23:59:59")
    )

    override def generate: Timestamp = {
      val milliSecondsSinceEpoch = (random.nextDouble() * (maxValue - minValue) + minValue).toLong
      Timestamp.from(Instant.ofEpochMilli(milliSecondsSinceEpoch))
    }

    private def getMinValue: Long = {
      Try(structField.metadata.getString(MINIMUM_VALUE)).map(Timestamp.valueOf)
        .getOrElse(Timestamp.from(Instant.now().minus(365, ChronoUnit.DAYS)))
        .toInstant.toEpochMilli
    }

    private def getMaxValue: Long = {
      Try(structField.metadata.getString(MAXIMUM_VALUE)).map(Timestamp.valueOf)
        .getOrElse(Timestamp.from(Instant.now()))
        .toInstant.toEpochMilli + 1L
    }
  }

  class RandomBooleanDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Boolean] {
    override def generate: Boolean = {
      random.nextBoolean()
    }
  }

  class RandomBinaryDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends NullableDataGenerator[Array[Byte]] {
    private lazy val minLength = Try(structField.metadata.getString(MINIMUM_LENGTH).toLong).getOrElse(1L).toInt
    private lazy val maxLength = Try(structField.metadata.getString(MAXIMUM_LENGTH).toLong).getOrElse(20L).toInt
    assert(minLength <= maxLength, s"minLength has to be less than or equal to maxLength, field-name${structField.name}")

    override val edgeCases: List[Array[Byte]] = List(Array(), "\n".getBytes, "\r".getBytes, "\t".getBytes,
      " ".getBytes, "\\u0000".getBytes, "\\ufff".getBytes, Array(Byte.MinValue), Array(Byte.MaxValue))

    override def generate: Array[Byte] = {
      val byteLength = (random.nextDouble() * (maxLength - minLength) + minLength).toInt
      faker.random().nextRandomBytes(byteLength)
    }
  }
}

