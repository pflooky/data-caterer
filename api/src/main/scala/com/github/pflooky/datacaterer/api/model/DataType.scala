package com.github.pflooky.datacaterer.api.model

trait DataType {
  override def toString: String = getClass.getSimpleName.toLowerCase.stripSuffix("type$")
}

class StringType extends DataType

case object StringType extends StringType

class IntegerType extends DataType

case object IntegerType extends IntegerType

class LongType extends DataType

case object LongType extends LongType

class ShortType extends DataType

case object ShortType extends ShortType

class DecimalType(precision: Int = 10, scale: Int = 0) extends DataType {
  assert(scale < precision, "Scale required to be less than precision")

  override def toString: String = s"decimal($precision, $scale)"
}

case object DecimalType extends DecimalType(10, 0)

class DoubleType extends DataType

case object DoubleType extends DoubleType

class FloatType extends DataType

case object FloatType extends FloatType

class DateType extends DataType

case object DateType extends DateType

class TimestampType extends DataType

case object TimestampType extends TimestampType

class BooleanType extends DataType

case object BooleanType extends BooleanType

class BinaryType extends DataType

case object BinaryType extends BinaryType

class ByteType extends DataType

case object ByteType extends ByteType

class ArrayType(`type`: DataType = StringType) extends DataType {
  override def toString: String = s"array<${`type`.toString}>"
}

case object ArrayType extends ArrayType(StringType)

class StructType extends DataType

case object StructType extends StructType
