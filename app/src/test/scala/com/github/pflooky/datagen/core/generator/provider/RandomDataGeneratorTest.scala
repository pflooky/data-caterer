package com.github.pflooky.datagen.core.generator.provider

import com.github.pflooky.datagen.core.exception.UnsupportedDataGeneratorType
import com.github.pflooky.datagen.core.generator.provider.RandomDataGenerator.{RandomDateDataGenerator, RandomDoubleDataGenerator, RandomIntDataGenerator, RandomStringDataGenerator, RandomTimestampDataGenerator}
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{Date, Timestamp}
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate}

class RandomDataGeneratorTest extends AnyFunSuite {

  test("Can get the correct data generator based on return type") {
    val stringGenerator = RandomDataGenerator.getGenerator("string", Map(), true)
    val stringCapitalWithSpaceGenerator = RandomDataGenerator.getGenerator(" STRING ", Map(), true)
    val intGenerator = RandomDataGenerator.getGenerator("int", Map(), true)
    val doubleGenerator = RandomDataGenerator.getGenerator("double", Map(), true)
    val dateGenerator = RandomDataGenerator.getGenerator("date", Map(), true)
    val timestampGenerator = RandomDataGenerator.getGenerator("timestamp", Map(), true)

    assert(stringGenerator.isInstanceOf[RandomStringDataGenerator])
    assert(stringCapitalWithSpaceGenerator.isInstanceOf[RandomStringDataGenerator])
    assert(intGenerator.isInstanceOf[RandomIntDataGenerator])
    assert(doubleGenerator.isInstanceOf[RandomDoubleDataGenerator])
    assert(dateGenerator.isInstanceOf[RandomDateDataGenerator])
    assert(timestampGenerator.isInstanceOf[RandomTimestampDataGenerator])
    assertThrows[UnsupportedDataGeneratorType](RandomDataGenerator.getGenerator("null", Map(), true))
  }

  test("Can create random string generator") {
    val stringGenerator = new RandomStringDataGenerator(Map(), false)
    val sampleData = stringGenerator.generate

    assert(stringGenerator.edgeCases.nonEmpty)
    assert(sampleData.nonEmpty)
    assert(sampleData.length <= 20)
  }

  test("Can create random int generator") {
    val intGenerator = new RandomIntDataGenerator(Map())
    val sampleData = intGenerator.generate

    assert(intGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 0)
    assert(sampleData <= 1)
  }

  test("Can create random double generator") {
    val doubleGenerator = new RandomDoubleDataGenerator(Map())
    val sampleData = doubleGenerator.generate

    assert(doubleGenerator.edgeCases.nonEmpty)
    assert(sampleData >= 0.0)
    assert(sampleData <= 1.0)
  }

  test("Can create random date generator") {
    val dateGenerator = new RandomDateDataGenerator(Map(), false)
    val sampleData = dateGenerator.generate

    assert(dateGenerator.edgeCases.nonEmpty)
    assert(sampleData.before(Date.valueOf(LocalDate.now())))
    assert(sampleData.after(Date.valueOf(LocalDate.now().minusDays(6))))
  }

  test("Can create random timestamp generator") {
    val dateGenerator = new RandomTimestampDataGenerator(Map(), false)
    val sampleData = dateGenerator.generate

    assert(dateGenerator.edgeCases.nonEmpty)
    assert(sampleData.before(Timestamp.from(Instant.now())))
    assert(sampleData.after(Timestamp.from(Instant.now().minus(6, ChronoUnit.DAYS))))
  }
}
