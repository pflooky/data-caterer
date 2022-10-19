package com.github.pflooky.datagen.core.generator.provider

import scala.util.Random

trait DataGenerator[T] extends Serializable {

  val PROBABILITY_OF_NULL = 0.1
  val PROBABILITY_OF_EDGE_CASES = 0.5

  val options: Map[String, Any]
  val edgeCases: List[T] = List()

  def generate: T

  lazy val random: Random = options.get("seed").map(x => new Random(x.asInstanceOf[Int])).getOrElse(new Random())

  def generateWrapper(randDouble: Double): T = {
    if (randDouble <= PROBABILITY_OF_EDGE_CASES) {
      edgeCases(random.nextInt(edgeCases.size))
    } else {
      generate
    }
  }
}

trait NullableDataGenerator[T >: Null] extends DataGenerator[T] {

  val isNullable: Boolean

  override def generateWrapper(randDouble: Double): T = {
    if (randDouble <= PROBABILITY_OF_NULL && isNullable) {
      null
    } else if (edgeCases.nonEmpty && (randDouble <= PROBABILITY_OF_EDGE_CASES + PROBABILITY_OF_NULL && isNullable) ||
      (randDouble <= PROBABILITY_OF_EDGE_CASES && !isNullable)) {
      edgeCases(random.nextInt(edgeCases.size))
    } else {
      generate
    }
  }
}