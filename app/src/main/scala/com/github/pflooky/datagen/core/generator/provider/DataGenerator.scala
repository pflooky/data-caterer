package com.github.pflooky.datagen.core.generator.provider

import com.github.pflooky.datacaterer.api.model.Constants.{ENABLED_EDGE_CASE, ENABLED_NULL, IS_UNIQUE, LIST_MAXIMUM_LENGTH, LIST_MINIMUM_LENGTH, PROBABILITY_OF_EDGE_CASE, PROBABILITY_OF_NULL, RANDOM_SEED, STATIC}
import net.datafaker.Faker
import org.apache.spark.sql.functions.{expr, rand, when}
import org.apache.spark.sql.types.StructField

import scala.collection.mutable
import scala.language.higherKinds
import scala.util.Random

trait DataGenerator[T] extends Serializable {

  val structField: StructField
  val faker: Faker


  val edgeCases: List[T] = List()

  def generate: T

  def generateSqlExpression: String

  lazy val optRandomSeed: Option[Long] = if (structField.metadata.contains(RANDOM_SEED)) Some(structField.metadata.getString(RANDOM_SEED).toLong) else None
  lazy val sqlRandom: String = optRandomSeed.map(seed => s"RAND($seed)").getOrElse("RAND()")
  lazy val random: Random = if (structField.metadata.contains(RANDOM_SEED)) new Random(structField.metadata.getString(RANDOM_SEED).toLong) else new Random()
  lazy val enabledNull: Boolean = if (structField.metadata.contains(ENABLED_NULL)) structField.metadata.getString(ENABLED_NULL).toBoolean else false
  lazy val enabledEdgeCases: Boolean = if (structField.metadata.contains(ENABLED_EDGE_CASE)) structField.metadata.getString(ENABLED_EDGE_CASE).toBoolean else false
  lazy val isUnique: Boolean = if (structField.metadata.contains(IS_UNIQUE)) structField.metadata.getString(IS_UNIQUE).toBoolean else false
  lazy val probabilityOfNull: Double = if (structField.metadata.contains(PROBABILITY_OF_NULL)) structField.metadata.getString(PROBABILITY_OF_NULL).toDouble else 0.1
  lazy val probabilityOfEdgeCases: Double = if (structField.metadata.contains(PROBABILITY_OF_EDGE_CASE)) structField.metadata.getString(PROBABILITY_OF_EDGE_CASE).toDouble else 0.5
  lazy val prevGenerated: mutable.Set[T] = mutable.Set[T]()
  lazy val optStatic: Option[String] = if (structField.metadata.contains(STATIC)) Some(structField.metadata.getString(STATIC)) else None

  def generateSqlExpressionWrapper: String = {
    if (optStatic.isDefined) {
      return s"'${optStatic.get}'"
    }
    val baseSqlExpression = generateSqlExpression
    val caseRandom = optRandomSeed.map(s => rand(s)).getOrElse(rand())
    (enabledEdgeCases, enabledNull) match {
      case (true, true) =>
        when(caseRandom.leq(probabilityOfEdgeCases), edgeCases(random.nextInt(edgeCases.size)))
          .otherwise(when(caseRandom.leq(probabilityOfEdgeCases + probabilityOfNull), null))
          .otherwise(expr(baseSqlExpression))
          .expr.sql
      case (true, false) =>
        when(caseRandom.leq(probabilityOfEdgeCases), edgeCases(random.nextInt(edgeCases.size)))
          .otherwise(expr(baseSqlExpression))
          .expr.sql
      case (false, true) =>
        when(caseRandom.leq(probabilityOfNull), null)
          .otherwise(expr(baseSqlExpression))
          .expr.sql
      case _ => baseSqlExpression
    }
  }

  def generateWrapper(count: Int = 0): T = {
    val randDouble = random.nextDouble()
    val generatedValue = if (enabledEdgeCases && randDouble <= probabilityOfEdgeCases) {
      edgeCases(random.nextInt(edgeCases.size))
    } else {
      generate
    }
    if (count > 10) {
      //TODO: logic doesn't work when field is auto_increment, need to be aware if data system automatically takes care of it (therefore, field can be omitted from generation)
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
    if (enabledNull && structField.nullable && randDouble <= probabilityOfNull) {
      null
    } else if (enabledEdgeCases && edgeCases.nonEmpty &&
      ((structField.nullable && randDouble <= probabilityOfEdgeCases + probabilityOfNull) ||
        (!structField.nullable && randDouble <= probabilityOfEdgeCases))) {
      edgeCases(random.nextInt(edgeCases.size))
    } else {
      generate
    }
  }
}

trait ListDataGenerator[T] extends NullableDataGenerator[List[T]] {

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