package com.github.pflooky.datagen.core.generator.metadata.validation

import com.github.pflooky.datacaterer.api.model.{ExpressionValidation, Validation}
import org.apache.spark.sql.types.{DataType, DateType, StringType, StructField, TimestampType}
import org.scalatest.funsuite.AnyFunSuite

class DateTimestampValidationPredictionCheckTest extends AnyFunSuite {

  test("Can create validations from data source with open_date and close_date fields") {
    checkResult("open_date")
  }

  test("Can create validations from data source with create_date and close_date fields") {
    checkResult("create_date")
  }

  test("Can create validations from data source with created_date and close_date fields") {
    checkResult("created_date")
  }

  test("Can create validations from data source with updated_date and close_date fields") {
    checkResult("updated_date")
  }

  test("Can create validations from data source with update_date and close_date fields") {
    checkResult("update_date")
  }

  test("Can create validations from data source with update_date and end_date fields") {
    checkResult("update_date", "end_date")
  }

  test("Can create validations from data source with update_ts and close_ts fields") {
    checkResult("update_ts", "end_ts", TimestampType)
  }

  test("Can create validations from data source with create_ts and close_ts fields") {
    checkResult("create_ts", "close_ts", TimestampType)
  }

  test("Can create validations from data source with start_ts and stop_ts fields") {
    checkResult("start_ts", "stop_ts", TimestampType)
  }

  test("Can create validations from data source with my_start and my_stop fields") {
    checkResult("my_start", "my_stop", TimestampType)
  }

  test("Can create validations from data source with my_create_txn and my_end_txn fields") {
    checkResult("my_create_txn", "my_end_txn", TimestampType)
  }

  //TODO detect if validation is not required
  test("Can create multiple validations from data source with my_create_txn, my_update_txn and my_end_txn fields") {
    val structFields = Array(StructField("id", StringType), StructField("my_create_txn", DateType), StructField("my_update_txn", DateType), StructField("my_end_txn", DateType))
    val result = new DateTimestampValidationPredictionCheck().check(structFields)

    assert(result.size == 3)
    val expectedExpressions = List(
      "DATE(my_create_txn) <= DATE(my_update_txn)",
      "DATE(my_update_txn) <= DATE(my_end_txn)",
      "DATE(my_create_txn) <= DATE(my_end_txn)"
    )
    result.foreach(v => {
      assert(v.validation.isInstanceOf[ExpressionValidation])
      assert(expectedExpressions.contains(v.validation.asInstanceOf[ExpressionValidation].expr))
    })
  }

  private def checkResult(firstColName: String, secondColName: String = "close_date", dataType: DataType = DateType): Unit = {
    val structFields = Array(StructField("id", StringType), StructField(firstColName, dataType), StructField(secondColName, dataType))
    val result = new DateTimestampValidationPredictionCheck().check(structFields)

    assert(result.nonEmpty)
    assert(result.head.validation.isInstanceOf[ExpressionValidation])
    assert(result.head.validation.asInstanceOf[ExpressionValidation].expr == s"${dataType.sql}($firstColName) <= ${dataType.sql}($secondColName)")
  }
}
