package com.github.pflooky.datagen.core.generator.provider

import com.github.pflooky.datacaterer.api.model.Constants.{DEFAULT_VALUE, DISTINCT_COUNT, EXPRESSION, LIST_MAXIMUM_LENGTH, LIST_MINIMUM_LENGTH, MAXIMUM, MAXIMUM_LENGTH, MINIMUM, MINIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, ROW_COUNT}
import com.github.pflooky.datagen.core.exception.UnsupportedDataGeneratorType
import com.github.pflooky.datagen.core.model.Constants._
import net.datafaker.Faker
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate}
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
      case ByteType => new RandomByteDataGenerator(structField, faker)
      case ArrayType(dt, _) => new RandomListDataGenerator(structField, dt, faker)
      case StructType(_) => new RandomStructTypeDataGenerator(structField, faker)
      case x => throw new UnsupportedDataGeneratorType(s"Unsupported type for random data generation: name=${structField.name}, type=${x.typeName}")
    }
  }

  class RandomStringDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends NullableDataGenerator[String] {
    private lazy val minLength = tryGetValue(structField.metadata, MINIMUM_LENGTH, 1)
    private lazy val maxLength = tryGetValue(structField.metadata, MAXIMUM_LENGTH, 10)
    assert(minLength <= maxLength, s"minLength has to be less than or equal to maxLength, field-name=${structField.name}, minLength=$minLength, maxLength=$maxLength")
    private lazy val tryExpression = Try(structField.metadata.getString(EXPRESSION))

    override val edgeCases: List[String] = List("", "\n", "\r", "\t", " ", "\\u0000", "\\ufff",
      "İyi günler", "Спасибо", "Καλημέρα", "صباح الخير", "Förlåt", "你好吗", "Nhà vệ sinh ở đâu", "こんにちは", "नमस्ते", "Բարեւ", "Здравейте")

    override def generate: String = {
      if (tryExpression.isSuccess) {
        faker.expression(tryExpression.get)
      } else {
        val stringLength = (random.nextDouble() * (maxLength - minLength) + minLength).toInt
        random.alphanumeric.take(stringLength).mkString
      }
    }

    override def generateSqlExpression: String = {
      if (tryExpression.isSuccess) {
        s"$GENERATE_FAKER_EXPRESSION_UDF('${tryExpression.get}')"
      } else {
        val randLength = s"CAST(ROUND($sqlRandom * ${maxLength - minLength} + $minLength, 0) AS INT)"
        s"SUBSTRING(ARRAY_JOIN(SHUFFLE(SPLIT(REGEXP_REPLACE(BASE64(MD5(CONCAT($sqlRandom, CURRENT_TIMESTAMP()))), '\\\\+|/|=', ' '), '[.]')), ''), 0, $randLength)"
      }
    }
  }

  class RandomIntDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Int] {
    private lazy val min = tryGetValue(structField.metadata, MINIMUM, 0)
    private lazy val max = tryGetValue(structField.metadata, MAXIMUM, 100000)
    assert(min <= max, s"min has to be less than or equal to max, field-name=${structField.name}, min=$min, max=$max")

    override val edgeCases: List[Int] = List(Int.MaxValue, Int.MinValue, 0)

    override def generate: Int = {
      faker.random().nextInt(min, max)
    }

    override def generateSqlExpression: String = {
      val defaultValue = tryGetValue(structField.metadata, DEFAULT_VALUE, "")
      if (defaultValue.startsWith("nextval(")) {
        s"CAST($max + $INDEX_INC_COL AS INT)"
      } else {
        s"CAST(ROUND($sqlRandom * ${max - min} + $min, 0) AS INT)"
      }
    }
  }

  class RandomShortDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Short] {
    private lazy val min = tryGetValue(structField.metadata, MINIMUM, 0)
    private lazy val max = tryGetValue(structField.metadata, MAXIMUM, 1000)
    assert(min <= max, s"min has to be less than or equal to max, field-name=${structField.name}, min=$min, max=$max")

    override val edgeCases: List[Short] = List(Short.MaxValue, Short.MinValue, 0)

    override def generate: Short = {
      (random.nextDouble() * (max - min) + min).toShort
    }

    override def generateSqlExpression: String = {
      val distinctCount = tryGetValue(structField.metadata, DISTINCT_COUNT, 0)
      val count = tryGetValue(structField.metadata, ROW_COUNT, 0)
      if (distinctCount == count) {
        s"CAST($max + $INDEX_INC_COL AS SHORT)"
      } else {
        s"CAST(ROUND($sqlRandom * ${max - min} + $min, 0) AS SHORT)"
      }
    }
  }

  class RandomLongDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Long] {
    private lazy val min = tryGetValue(structField.metadata, MINIMUM, 0L)
    private lazy val max = tryGetValue(structField.metadata, MAXIMUM, 100000L)
    assert(min <= max, s"min has to be less than or equal to max, field-name=${structField.name}, min=$min, max=$max")

    override val edgeCases: List[Long] = List(Long.MaxValue, Long.MinValue, 0)

    override def generate: Long = {
      faker.random().nextLong(min, max)
    }

    override def generateSqlExpression: String = {
      s"CAST(ROUND($sqlRandom * ${max - min} + $min, 0) AS LONG)"
    }
  }

  class RandomDecimalDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[BigDecimal] {
    private lazy val min = tryGetValue(structField.metadata, MINIMUM, BigDecimal.valueOf(0))
    private lazy val max = tryGetValue(structField.metadata, MAXIMUM, BigDecimal.valueOf(100000))
    private lazy val precision = tryGetValue(structField.metadata, NUMERIC_PRECISION, 38)
    private lazy val scale = tryGetValue(structField.metadata, NUMERIC_SCALE, 18)
    assert(min <= max, s"min has to be less than or equal to max, field-name=${structField.name}, min=$min, max=$max")

    override val edgeCases: List[BigDecimal] = List(Long.MaxValue, Long.MinValue, 0)

    override def generate: BigDecimal = {
      random.nextDouble() * (max - min) + min
    }

    override def generateSqlExpression: String = {
      s"CAST($sqlRandom * ${max - min} + $min AS DECIMAL($precision, $scale))"
    }
  }

  class RandomDoubleDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Double] {
    private lazy val min = tryGetValue(structField.metadata, MINIMUM, 0.0)
    private lazy val max = tryGetValue(structField.metadata, MAXIMUM, 100000.0)
    assert(min <= max, s"min has to be less than or equal to max, field-name=${structField.name}, min=$min, max=$max")

    override val edgeCases: List[Double] = List(Double.PositiveInfinity, Double.MaxValue, Double.MinPositiveValue,
      0.0, -0.0, Double.MinValue, Double.NegativeInfinity, Double.NaN)

    override def generate: Double = {
      faker.random().nextDouble(min, max)
    }

    override def generateSqlExpression: String = {
      s"CAST($sqlRandom * ${max - min} + $min AS DOUBLE)"
    }
  }

  class RandomFloatDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Float] {
    private lazy val min = tryGetValue(structField.metadata, MINIMUM, 0.0.toFloat)
    private lazy val max = tryGetValue(structField.metadata, MAXIMUM, 100000.0.toFloat)
    assert(min <= max, s"min has to be less than or equal to max, field-name=${structField.name}, min=$min, max=$max")

    override val edgeCases: List[Float] = List(Float.PositiveInfinity, Float.MaxValue, Float.MinPositiveValue,
      0.0f, -0.0f, Float.MinValue, Float.NegativeInfinity, Float.NaN)

    override def generate: Float = {
      faker.random().nextDouble(min, max).toFloat
    }

    override def generateSqlExpression: String = {
      s"CAST($sqlRandom * ${max - min} + $min AS FLOAT)"
    }
  }

  class RandomDateDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends NullableDataGenerator[Date] {
    private lazy val min = getMinValue
    private lazy val max = getMaxValue
    assert(min.isBefore(max) || min.isEqual(max), s"min has to be less than or equal to max, field-name=${structField.name}, min=$min, max=$max")
    private lazy val maxDays = java.time.temporal.ChronoUnit.DAYS.between(min, max).toInt

    //from here: https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/RandomDataGenerator.scala#L206
    override val edgeCases: List[Date] = List(
      Date.valueOf("0001-01-01"),
      Date.valueOf("1582-10-15"),
      Date.valueOf("1970-01-01"),
      Date.valueOf("9999-12-31")
    )

    override def generate: Date = {
      Date.valueOf(min.plusDays(random.nextInt(maxDays)))
    }

    private def getMinValue: LocalDate = {
      Try(structField.metadata.getString(MINIMUM)).map(LocalDate.parse)
        .getOrElse(LocalDate.now().minusDays(365))
    }

    private def getMaxValue: LocalDate = {
      Try(structField.metadata.getString(MAXIMUM)).map(LocalDate.parse)
        .getOrElse(LocalDate.now())
    }

    override def generateSqlExpression: String = {
      s"DATE_ADD('${min.toString}', CAST($sqlRandom * $maxDays AS INT))"
    }
  }

  class RandomTimestampDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends NullableDataGenerator[Timestamp] {
    private lazy val min = getMinValue
    private lazy val max = getMaxValue
    assert(min <= max, s"min has to be less than or equal to max, field-name=${structField.name}, min=$min, max=$max")

    //from here: https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/RandomDataGenerator.scala#L159
    override val edgeCases: List[Timestamp] = List(
      Timestamp.valueOf("0001-01-01 00:00:00"),
      Timestamp.valueOf("1582-10-15 23:59:59"),
      Timestamp.valueOf("1970-01-01 00:00:00"),
      Timestamp.valueOf("9999-12-31 23:59:59")
    )

    override def generate: Timestamp = {
      val milliSecondsSinceEpoch = (random.nextDouble() * (max - min) + min).toLong
      Timestamp.from(Instant.ofEpochMilli(milliSecondsSinceEpoch))
    }

    private def getMinValue: Long = {
      Try(structField.metadata.getString(MINIMUM)).map(Timestamp.valueOf)
        .getOrElse(Timestamp.from(Instant.now().minus(365, ChronoUnit.DAYS)))
        .toInstant.toEpochMilli
    }

    private def getMaxValue: Long = {
      Try(structField.metadata.getString(MAXIMUM)).map(Timestamp.valueOf)
        .getOrElse(Timestamp.from(Instant.now()))
        .toInstant.toEpochMilli + 1L
    }

    override def generateSqlExpression: String = {
      s"TIMESTAMP_MILLIS(CAST($sqlRandom * ${max - min} + $min AS LONG))"
    }
  }

  class RandomBooleanDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Boolean] {
    override def generate: Boolean = {
      random.nextBoolean()
    }

    override def generateSqlExpression: String = {
      s"BOOLEAN(ROUND($sqlRandom))"
    }
  }

  class RandomBinaryDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends NullableDataGenerator[Array[Byte]] {
    private lazy val minLength = tryGetValue(structField.metadata, MINIMUM_LENGTH, 1)
    private lazy val maxLength = tryGetValue(structField.metadata, MAXIMUM_LENGTH, 20)
    assert(minLength <= maxLength, s"minLength has to be less than or equal to maxLength, field-name=${structField.name}, minLength=$minLength, maxLength=$maxLength")

    override val edgeCases: List[Array[Byte]] = List(Array(), "\n".getBytes, "\r".getBytes, "\t".getBytes,
      " ".getBytes, "\\u0000".getBytes, "\\ufff".getBytes, Array(Byte.MinValue), Array(Byte.MaxValue))

    override def generate: Array[Byte] = {
      val byteLength = (random.nextDouble() * (maxLength - minLength) + minLength).toInt
      faker.random().nextRandomBytes(byteLength)
    }

    override def generateSqlExpression: String = {
      s"TO_BINARY(ARRAY_JOIN(TRANSFORM(ARRAY_REPEAT(1, CAST($sqlRandom * ${maxLength - minLength} + $minLength AS INT)), x -> CHAR(ROUND($sqlRandom * 94 + 32, 0))), ''), 'utf-8')"
    }
  }

  class RandomByteDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Byte] {
    override val edgeCases: List[Byte] = List(Byte.MinValue, Byte.MaxValue)

    override def generate: Byte = {
      faker.random().nextRandomBytes(1).head
    }

    override def generateSqlExpression: String = {
      s"TO_BINARY(CHAR(ROUND($sqlRandom * 94 + 32, 0)))"
    }
  }

  class RandomListDataGenerator[T](val structField: StructField, val dataType: DataType, val faker: Faker = new Faker()) extends ListDataGenerator[T] {
    override lazy val listMinSize: Int = tryGetValue(structField.metadata, LIST_MINIMUM_LENGTH, 0)
    override lazy val listMaxSize: Int = tryGetValue(structField.metadata, LIST_MAXIMUM_LENGTH, 5)

    override def elementGenerator: DataGenerator[T] = {
      dataType match {
        case structType: StructType =>
          new RandomStructTypeDataGenerator(StructField(structField.name, structType), faker).asInstanceOf[DataGenerator[T]]
        case _ =>
          getGeneratorForStructField(structField.copy(dataType = dataType), faker).asInstanceOf[DataGenerator[T]]
      }
    }

    override def generateSqlExpression: String = {
      val nestedSqlExpressions = dataType match {
        case structType: StructType =>
          val structGen = new RandomStructTypeDataGenerator(StructField(structField.name, structType))
          structGen.generateSqlExpression
        case _ =>
          getGeneratorForStructField(structField.copy(dataType = dataType)).generateSqlExpression
      }
      s"TRANSFORM(ARRAY_REPEAT(1, CAST($sqlRandom * ${listMaxSize - listMinSize} + $listMinSize AS INT)), x -> $nestedSqlExpressions)"
    }
  }

  class RandomStructTypeDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends DataGenerator[Row] {
    override def generate: Row = {
      structField.dataType match {
        case ArrayType(dt, _) =>
          val listGenerator = new RandomListDataGenerator(structField, dt, faker)
          Row.fromSeq(listGenerator.generate)
        case StructType(fields) =>
          val dataGenerators = fields.map(field => getGeneratorForStructField(field, faker))
          Row.fromSeq(dataGenerators.map(_.generateWrapper()))
      }
    }

    override def generateSqlExpression: String = {
      val nestedSqlExpression = structField.dataType match {
        case ArrayType(dt, _) =>
          val listGenerator = new RandomListDataGenerator(structField, dt)
          listGenerator.generateSqlExpression
        case StructType(fields) =>
          fields.map(getGeneratorForStructField(_))
            .map(f => s"'${f.structField.name}', ${f.generateSqlExpression}")
            .mkString(",")
        case _ =>
          getGeneratorForStructField(structField).generateSqlExpression
      }
      s"NAMED_STRUCT($nestedSqlExpression)"
    }
  }

  def tryGetValue[T](metadata: Metadata, key: String, default: T)(implicit converter: Converter[T]): T = {
    Try(converter.convert(metadata.getString(key + "Value")))
      .getOrElse(
        Try(converter.convert(metadata.getString(key)))
          .getOrElse(default)
      )
  }

  trait Converter[T] {
    self =>
    def convert(v: String): T
  }

  object Converter {
    implicit val stringLoader: Converter[String] = (v: String) => v

    implicit val intLoader: Converter[Int] = (v: String) => v.toInt

    implicit val longLoader: Converter[Long] = (v: String) => v.toLong

    implicit val shortLoader: Converter[Short] = (v: String) => v.toShort

    implicit val doubleLoader: Converter[Double] = (v: String) => v.toDouble

    implicit val floatLoader: Converter[Float] = (v: String) => v.toFloat

    implicit val decimalLoader: Converter[BigDecimal] = (v: String) => BigDecimal(v)
  }
}

