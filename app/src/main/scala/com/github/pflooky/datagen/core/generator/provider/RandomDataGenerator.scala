package com.github.pflooky.datagen.core.generator.provider

import com.github.pflooky.datagen.core.exception.UnsupportedDataGeneratorType
import com.github.pflooky.datagen.core.model.Constants.{MAXIMUM_LENGTH, MAXIMUM_VALUE, MINIMUM_LENGTH, MINIMUM_VALUE}
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate}
import scala.util.Try

object RandomDataGenerator {

  def getGeneratorForStructType(structType: StructType): Array[DataGenerator[_]] = {
    structType.fields.map(getGeneratorForStructField)
  }

  def getGeneratorForStructField(structField: StructField): DataGenerator[_] = {
    structField.dataType match {
      case StringType => new RandomStringDataGenerator(structField)
      case IntegerType => new RandomIntDataGenerator(structField)
      case DoubleType => new RandomDoubleDataGenerator(structField)
      case DateType => new RandomDateDataGenerator(structField)
      case TimestampType => new RandomTimestampDataGenerator(structField)
      case BooleanType => new RandomBooleanDataGenerator(structField)
      case x => throw new UnsupportedDataGeneratorType(s"Unsupported type for random data generating: type=${x.typeName}")
    }
  }

  class RandomStringDataGenerator(val structField: StructField) extends NullableDataGenerator[String] {
    private lazy val minLength = Try(structField.metadata.getLong(MINIMUM_LENGTH)).getOrElse(1L)
    private lazy val maxLength = Try(structField.metadata.getLong(MAXIMUM_LENGTH)).getOrElse(20L)

    override val edgeCases: List[String] = List("", "\n", "\r", "\t", " ")

    override def generate: String = {
      random.alphanumeric.take(random.between(minLength, maxLength).toInt).mkString
    }
  }

  class RandomIntDataGenerator(val structField: StructField) extends DataGenerator[Int] {
    private lazy val minValue = Try(structField.metadata.getLong(MINIMUM_VALUE).toInt).getOrElse(0)
    private lazy val maxValue = Try(structField.metadata.getLong(MAXIMUM_VALUE).toInt).getOrElse(1) + 1

    override val edgeCases: List[Int] = List(Int.MaxValue, Int.MinValue, 0)

    override def generate: Int = {
      random.between(minValue, maxValue)
    }
  }

  class RandomDoubleDataGenerator(val structField: StructField) extends DataGenerator[Double] {
    private lazy val minValue = Try(structField.metadata.getDouble(MINIMUM_VALUE)).getOrElse(0.0)
    private lazy val maxValue = Try(structField.metadata.getDouble(MAXIMUM_VALUE)).getOrElse(1.0)

    override val edgeCases: List[Double] = List(Double.PositiveInfinity, Double.MaxValue, Double.MinPositiveValue,
      0.0, -0.0, Double.MinValue, Double.NegativeInfinity)

    override def generate: Double = {
      random.between(minValue, maxValue)
    }
  }

  class RandomDateDataGenerator(val structField: StructField) extends NullableDataGenerator[Date] {
    private lazy val minValue = getMinValue
    private lazy val maxValue = getMaxValue
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
        .getOrElse(LocalDate.now().minusDays(5))
    }

    private def getMaxValue: LocalDate = {
      Try(structField.metadata.getString(MAXIMUM_VALUE)).map(LocalDate.parse)
        .getOrElse(LocalDate.now())
    }
  }

  class RandomTimestampDataGenerator(val structField: StructField) extends NullableDataGenerator[Timestamp] {
    private lazy val minValue = getMinValue
    private lazy val maxValue = getMaxValue

    //from here: https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/RandomDataGenerator.scala#L159
    override val edgeCases: List[Timestamp] = List(
      Timestamp.valueOf("0001-01-01 00:00:00"),
      Timestamp.valueOf("1582-10-15 23:59:59"),
      Timestamp.valueOf("1970-01-01 00:00:00"),
      Timestamp.valueOf("9999-12-31 23:59:59")
    )

    override def generate: Timestamp = {
      val milliSecondsSinceEpoch = random.between(minValue, maxValue)
      Timestamp.from(Instant.ofEpochMilli(milliSecondsSinceEpoch))
    }

    private def getMinValue: Long = {
      Try(structField.metadata.getString(MINIMUM_VALUE)).map(Timestamp.valueOf)
        .getOrElse(Timestamp.from(Instant.now().minus(5, ChronoUnit.DAYS)))
        .toInstant.toEpochMilli
    }

    private def getMaxValue: Long = {
      Try(structField.metadata.getString(MAXIMUM_VALUE)).map(Timestamp.valueOf)
        .getOrElse(Timestamp.from(Instant.now()))
        .toInstant.toEpochMilli + 1L
    }
  }

  class RandomBooleanDataGenerator(val structField: StructField) extends DataGenerator[Boolean] {
    override def generate: Boolean = {
      random.nextBoolean()
    }
  }
}

