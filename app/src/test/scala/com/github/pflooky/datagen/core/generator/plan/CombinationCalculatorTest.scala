package com.github.pflooky.datagen.core.generator.plan

import com.github.pflooky.datagen.core.model.{Field, Generator, Schema}
import net.datafaker.Faker
import org.scalatest.funsuite.AnyFunSuite

class CombinationCalculatorTest extends AnyFunSuite {

  test("Can calculate number of combinations given a schema with faker expressions and one of data generators") {
    val schema = Schema("manual", Some(List(
      Field("account_id", Some("string"), Some(Generator("random"))),
      Field("name", Some("string"), Some(Generator("random", Map("expression" -> "#{Name.name}")))),
      Field("status", Some("string"), Some(Generator("oneOf", Map("oneOf" -> List("open", "closed"))))),
    )))
    val faker = new Faker()

    val result = CombinationCalculator.totalCombinationsForSchema(schema, faker)

    assert(result.isDefined)
    assert(result.get == BigInt(103908640))
  }

}
