package com.github.pflooky.datagen.core.generator.provider

import com.github.pflooky.datagen.core.model.Constants.{ENABLED_EDGE_CASES, ENABLED_NULL, RANDOM_SEED}
import org.apache.spark.sql.types.StructField

import scala.util.Random

trait DataGenerator[T] extends Serializable {

  val structField: StructField

  val PROBABILITY_OF_NULL = 0.1
  val PROBABILITY_OF_EDGE_CASES = 0.5

  val edgeCases: List[T] = List()

  def generate: T

  lazy val random: Random = if (structField.metadata.contains(RANDOM_SEED)) new Random(structField.metadata.getLong(RANDOM_SEED)) else new Random()
  lazy val enabledNull: Boolean = if (structField.metadata.contains(ENABLED_NULL)) structField.metadata.getBoolean(ENABLED_NULL) else false
  lazy val enabledEdgeCases: Boolean = if (structField.metadata.contains(ENABLED_EDGE_CASES)) structField.metadata.getBoolean(ENABLED_EDGE_CASES) else false

  def generateWrapper: T = {
    val randDouble = random.nextDouble()
    if (enabledEdgeCases && randDouble <= PROBABILITY_OF_EDGE_CASES) {
      edgeCases(random.nextInt(edgeCases.size))
    } else {
      generate
    }
  }
}

trait NullableDataGenerator[T >: Null] extends DataGenerator[T] {

  override def generateWrapper: T = {
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