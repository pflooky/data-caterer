package com.github.pflooky.datagen.core.model

import com.github.pflooky.datacaterer.api.ValidationBuilder
import com.github.pflooky.datagen.core.model.ValidationImplicits.ValidationOps
import com.github.pflooky.datagen.core.util.{SparkSuite, Transaction}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.sql.Date

@RunWith(classOf[JUnitRunner])
class ValidationImplicitsTest extends SparkSuite {

  private val sampleData = Seq(
    Transaction("acc123", "peter", "txn1", Date.valueOf("2020-01-01"), 10.0),
    Transaction("acc123", "peter", "txn2", Date.valueOf("2020-01-01"), 50.0),
    Transaction("acc123", "peter", "txn3", Date.valueOf("2020-01-01"), 200.0),
    Transaction("acc123", "peter", "txn4", Date.valueOf("2020-01-01"), 500.0)
  )
  private val df = sparkSession.createDataFrame(sampleData)

  test("Can return empty sample rows when validation is successful") {
    val validation = new ValidationBuilder().expr("amount < 1000").validation
    val result = validation.validate(df, 4)

    assert(result.isSuccess)
    assert(result.sampleErrorValues.isEmpty)
  }

  test("Can return empty sample rows when validation is successful from error threshold") {
    val validation = new ValidationBuilder().expr("amount < 400").errorThreshold(1).validation
    val result = validation.validate(df, 4)

    assert(result.isSuccess)
    assert(result.sampleErrorValues.isEmpty)
  }

  test("Can get sample rows when validation is not successful") {
    val validation = new ValidationBuilder().expr("amount < 100").validation
    val result = validation.validate(df, 4)

    assert(!result.isSuccess)
    assert(result.sampleErrorValues.isDefined)
    assert(result.sampleErrorValues.get.count() == 2)
    assert(result.sampleErrorValues.get.filter(r => r.getAs[Double]("amount") >= 100).count() == 2)
  }

  test("Can get sample rows when validation is not successful by error threshold greater than 1") {
    val validation = new ValidationBuilder().expr("amount < 20").errorThreshold(2).validation
    val result = validation.validate(df, 4)

    assert(!result.isSuccess)
    assert(result.sampleErrorValues.isDefined)
    assert(result.sampleErrorValues.get.count() == 3)
    assert(result.sampleErrorValues.get.filter(r => r.getAs[Double]("amount") >= 20).count() == 3)
  }

  test("Can get sample rows when validation is not successful by error threshold less than 1") {
    val validation = new ValidationBuilder().expr("amount < 100").errorThreshold(0.1).validation
    val result = validation.validate(df, 4)

    assert(!result.isSuccess)
    assert(result.sampleErrorValues.isDefined)
    assert(result.sampleErrorValues.get.count() == 2)
    assert(result.sampleErrorValues.get.filter(r => r.getAs[Double]("amount") >= 100).count() == 2)
  }
}
