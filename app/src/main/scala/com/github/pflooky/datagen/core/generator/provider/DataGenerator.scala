package com.github.pflooky.datagen.core.generator.provider

import scala.util.Random

trait DataGenerator[T] extends Serializable {

  val PROBABILITY_OF_NULL = 0.1
  val PROBABILITY_OF_EDGE_CASES = 0.5

  val options: Map[String, Any]
  val edgeCases: List[T] = List()

  def generate: T

  lazy val random: Random = options.get("seed").map(x => new Random(x.asInstanceOf[Int])).getOrElse(new Random())
  lazy val enabledNull: Boolean = options.getOrElse("enableNull", "false").toString.toBoolean
  lazy val enabledEdgeCases: Boolean = options.getOrElse("enableEdgeCases", "false").toString.toBoolean

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

  val isNullable: Boolean

  override def generateWrapper: T = {
    val randDouble = random.nextDouble()
    if (enabledNull && isNullable && randDouble <= PROBABILITY_OF_NULL) {
      null
    } else if (enabledEdgeCases && edgeCases.nonEmpty &&
      ((isNullable && randDouble <= PROBABILITY_OF_EDGE_CASES + PROBABILITY_OF_NULL) ||
      (!isNullable && randDouble <= PROBABILITY_OF_EDGE_CASES))) {
      edgeCases(random.nextInt(edgeCases.size))
    } else {
      generate
    }
  }
}