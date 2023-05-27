package com.github.pflooky.datagen.core.generator.provider

import com.github.pflooky.datagen.core.model.Constants.{ENABLED_EDGE_CASES, ENABLED_NULL, IS_UNIQUE, LIST_MAXIMUM_LENGTH, LIST_MINIMUM_LENGTH, MAXIMUM_LENGTH, RANDOM_SEED}
import net.datafaker.Faker
import org.apache.spark.sql.types.{DataType, StructField}

import scala.collection.mutable
import scala.language.higherKinds
import scala.util.Random

trait DataGenerator[T] extends Serializable {

  val structField: StructField
  val faker: Faker

  val PROBABILITY_OF_NULL = 0.1
  val PROBABILITY_OF_EDGE_CASES = 0.5

  val edgeCases: List[T] = List()

  def generate: T

  lazy val random: Random = if (structField.metadata.contains(RANDOM_SEED)) new Random(structField.metadata.getString(RANDOM_SEED).toLong) else new Random()
  lazy val enabledNull: Boolean = if (structField.metadata.contains(ENABLED_NULL)) structField.metadata.getString(ENABLED_NULL).toBoolean else false
  lazy val enabledEdgeCases: Boolean = if (structField.metadata.contains(ENABLED_EDGE_CASES)) structField.metadata.getString(ENABLED_EDGE_CASES).toBoolean else false
  lazy val isUnique: Boolean = if (structField.metadata.contains(IS_UNIQUE)) structField.metadata.getString(IS_UNIQUE).toBoolean else false
  lazy val prevGenerated: mutable.Set[T] = mutable.Set[T]()

  def generateWrapper(count: Int = 0): T = {
    val randDouble = random.nextDouble()
    val generatedValue = if (enabledEdgeCases && randDouble <= PROBABILITY_OF_EDGE_CASES) {
      edgeCases(random.nextInt(edgeCases.size))
    } else {
      generate
    }
    if (count > 10) {
      throw new RuntimeException(s"Failed to generate new unique value for field, retries=$count, name=${structField.name}, " +
        s"metadata=${structField.metadata}, sample-previously-generated=${prevGenerated.take(3)}")
    } else if (isUnique) {
      if (prevGenerated.contains(generatedValue)) {
        generateWrapper(count + 1)
      } else {
        prevGenerated.add(generatedValue)
        generatedValue
      }
    } else {
      generatedValue
    }
  }
}

trait NullableDataGenerator[T >: Null] extends DataGenerator[T] {

  override def generateWrapper(count: Int = 0): T = {
    val randDouble = random.nextDouble()
    if (enabledNull && structField.nullable && randDouble <= PROBABILITY_OF_NULL) {
      null
    } else if (enabledEdgeCases && edgeCases.nonEmpty &&
      ((structField.nullable && randDouble <= PROBABILITY_OF_EDGE_CASES + PROBABILITY_OF_NULL) ||
        (!structField.nullable && randDouble <= PROBABILITY_OF_EDGE_CASES))) {
      edgeCases(random.nextInt(edgeCases.size))
    } else {
      generate
    }
  }
}

trait ListDataGenerator[T] extends DataGenerator[List[T]] {

  lazy val listMaxSize: Int = if (structField.metadata.contains(LIST_MAXIMUM_LENGTH)) structField.metadata.getString(LIST_MAXIMUM_LENGTH).toInt else 5
  lazy val listMinSize: Int = if (structField.metadata.contains(LIST_MINIMUM_LENGTH)) structField.metadata.getString(LIST_MINIMUM_LENGTH).toInt else 0

  def elementGenerator: DataGenerator[T]

  override def generate: List[T] = {
    val listSize = random.nextInt(listMaxSize) + listMinSize
    (listMinSize to listSize)
      .map(_ => elementGenerator.generate)
      .toList
  }
}