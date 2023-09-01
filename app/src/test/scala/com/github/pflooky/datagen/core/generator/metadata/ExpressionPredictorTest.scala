package com.github.pflooky.datagen.core.generator.metadata

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.nio.file.{Files, Paths, StandardOpenOption}

@RunWith(classOf[JUnitRunner])
class ExpressionPredictorTest extends AnyFunSuite {

  test("Can get all data faker expressions and write to file") {
    val allExpressions = ExpressionPredictor.getAllFakerExpressionTypes.sorted
    val testResourcesFolder = getClass.getResource("/datafaker").getPath
    val file = Paths.get(s"$testResourcesFolder/expressions.txt")
    Files.write(file, allExpressions.mkString("\n").getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }
}