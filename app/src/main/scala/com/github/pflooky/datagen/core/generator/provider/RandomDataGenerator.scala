package com.github.pflooky.datagen.core.generator.provider

import com.github.pflooky.datagen.core.exception.UnsupportedDataGeneratorType

import java.sql.{Date, Timestamp}
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate}

object RandomDataGenerator {

  def getGenerator(returnType: String, options: Map[String, Any], isNullable: Boolean): DataGenerator[_] = {
    returnType.trim.toLowerCase match {
      case "string" => new RandomStringDataGenerator(options, isNullable)
      case "int" => new RandomIntDataGenerator(options)
      case "double" => new RandomDoubleDataGenerator(options)
      case "date" => new RandomDateDataGenerator(options, isNullable)
      case "timestamp" => new RandomTimestampDataGenerator(options, isNullable)
      case x => throw new UnsupportedDataGeneratorType(s"Unsupported type for random data generating: type=$x")
    }
  }

  class RandomStringDataGenerator(val options: Map[String, Any], val isNullable: Boolean) extends NullableDataGenerator[String] {
    private lazy val minLength = options.getOrElse("minLength", 1).asInstanceOf[Int]
    private lazy val maxLength = options.getOrElse("maxLength", 20).asInstanceOf[Int]

    override val edgeCases: List[String] = List("", "\n", "\r", "\t", " ")

    override def generate: String = {
      random.alphanumeric.take(random.between(minLength, maxLength)).mkString
    }
  }

  class RandomIntDataGenerator(val options: Map[String, Any]) extends DataGenerator[Int] {
    private lazy val minValue = options.getOrElse("minValue", 0).asInstanceOf[Int]
    private lazy val maxValue = options.getOrElse("maxValue", 1).asInstanceOf[Int] + 1

    override val edgeCases: List[Int] = List(Int.MaxValue, Int.MinValue, 0)

    override def generate: Int = {
      random.between(minValue, maxValue)
    }
  }

  class RandomDoubleDataGenerator(val options: Map[String, Any]) extends DataGenerator[Double] {
    private lazy val minValue = options.getOrElse("minValue", 0.0).asInstanceOf[Double]
    private lazy val maxValue = options.getOrElse("maxValue", 1.0).asInstanceOf[Double]

    override val edgeCases: List[Double] = List(Double.PositiveInfinity, Double.MaxValue, Double.MinPositiveValue,
      0.0, -0.0, Double.MinValue, Double.NegativeInfinity)

    override def generate: Double = {
      random.between(minValue, maxValue)
    }
  }

  class RandomDateDataGenerator(val options: Map[String, Any], val isNullable: Boolean) extends NullableDataGenerator[Date] {
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
      options.get("minValue")
        .map(x => LocalDate.parse(x.asInstanceOf[String]))
        .getOrElse(LocalDate.now().minusDays(5))
    }

    private def getMaxValue: LocalDate = {
      options.get("maxValue")
        .map(x => LocalDate.parse(x.asInstanceOf[String]))
        .getOrElse(LocalDate.now())
    }
  }

  class RandomTimestampDataGenerator(val options: Map[String, Any], val isNullable: Boolean) extends NullableDataGenerator[Timestamp] {
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
      val ts = options.get("minValue")
        .map(x => Timestamp.valueOf(x.asInstanceOf[String]))
        .getOrElse(Timestamp.from(Instant.now().minus(5, ChronoUnit.DAYS)))
      ts.toInstant.toEpochMilli
    }

    private def getMaxValue: Long = {
      val ts = options.get("maxValue")
        .map(x => Timestamp.valueOf(x.asInstanceOf[String]))
        .getOrElse(Timestamp.from(Instant.now()))
      ts.toInstant.toEpochMilli + 1L
    }
  }
}

