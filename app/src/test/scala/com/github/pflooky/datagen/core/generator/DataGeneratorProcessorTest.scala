package com.github.pflooky.datagen.core.generator

import com.github.pflooky.datagen.core.util.{FileUtil, SparkSuite}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import scala.reflect.io.Directory

@RunWith(classOf[JUnitRunner])
class DataGeneratorProcessorTest extends SparkSuite {

  test("Can parse plan and tasks, then execute data generation") {
    val basePath = "src/test/resources/sample/data"
    val dataGeneratorProcessor = new DataGeneratorProcessor()

    dataGeneratorProcessor.generateData()

    val generatedData = sparkSession.read
      .json(s"$basePath/generated/json/account-gen")
    val generatedCount = generatedData.count()
    assert(generatedCount > 0)
    val recordTrackingData = sparkSession.read
      .parquet(s"$basePath/recordTracking/json/account_json/app/src/test/resources/sample/data/generated/json/account-gen")
    val recordTrackCount = recordTrackingData.count()
    assert(recordTrackCount > 0 && recordTrackCount == generatedCount)
  }

}
