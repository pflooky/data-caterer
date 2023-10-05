package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.model.{ExpressionValidation, GroupByValidation, PauseWaitCondition}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.sql.{Date, Timestamp}

@RunWith(classOf[JUnitRunner])
class ValidationConfigurationBuilderTest extends AnyFunSuite {

  test("Can create simple validation for data source") {
    val result = ValidationConfigurationBuilder()
      .name("my validations")
      .description("check account data")
      .addValidations(
        "account_json",
        Map("path" -> "/my/data/path"),
        ValidationBuilder().expr("amount < 100"),
        ValidationBuilder().expr("name == 'Peter'")
      ).validationConfiguration

    assert(result.name == "my validations")
    assert(result.description == "check account data")
    assert(result.dataSources.size == 1)
    assert(result.dataSources.head._1 == "account_json")
    assert(result.dataSources.head._2.options == Map("path" -> "/my/data/path"))
    assert(result.dataSources.head._2.waitCondition == PauseWaitCondition())
    assert(result.dataSources.head._2.validations.size == 2)
    assert(result.dataSources.head._2.validations.map(_.validation).contains(ExpressionValidation("amount < 100")))
    assert(result.dataSources.head._2.validations.map(_.validation).contains(ExpressionValidation("name == 'Peter'")))
  }

  test("Can create column specific validation") {
    val result = ValidationBuilder().col("my_col").greaterThan(10)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col > 10")
  }

  test("Can create column equal to validation") {
    val result = ValidationBuilder().col("my_col").isEqual(10)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col == 10")

    val resultStr = ValidationBuilder().col("my_col").isEqual("created")

    assert(resultStr.validation.isInstanceOf[ExpressionValidation])
    assert(resultStr.validation.asInstanceOf[ExpressionValidation].expr == "my_col == 'created'")
  }

  test("Can create column not equal to validation") {
    val result = ValidationBuilder().col("my_col").isNotEqual(10)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col != 10")

    val resultStr = ValidationBuilder().col("my_col").isNotEqual("created")

    assert(resultStr.validation.isInstanceOf[ExpressionValidation])
    assert(resultStr.validation.asInstanceOf[ExpressionValidation].expr == "my_col != 'created'")
  }

  test("Can create column is null validation") {
    val result = ValidationBuilder().col("my_col").isNull

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "ISNULL(my_col)")
  }

  test("Can create column is not null validation") {
    val result = ValidationBuilder().col("my_col").isNotNull

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "ISNOTNULL(my_col)")
  }

  test("Can create column contains validation") {
    val result = ValidationBuilder().col("my_col").contains("apple")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "CONTAINS(my_col, 'apple')")
  }

  test("Can create column not contains validation") {
    val result = ValidationBuilder().col("my_col").notContains("apple")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "!CONTAINS(my_col, 'apple')")
  }

  test("Can create column less than validation") {
    val result = ValidationBuilder().col("my_col").lessThan(Date.valueOf("2023-01-01"))

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col < DATE('2023-01-01')")
  }

  test("Can create column less than or equal validation") {
    val result = ValidationBuilder().col("my_col").lessThanOrEqual(Timestamp.valueOf("2023-01-01 00:00:00.0"))

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col <= TIMESTAMP('2023-01-01 00:00:00.0')")
  }

  test("Can create column greater than validation") {
    val result = ValidationBuilder().col("my_col").greaterThan(10)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col > 10")
  }

  test("Can create column greater than or equal validation") {
    val result = ValidationBuilder().col("my_col").greaterThanOrEqual(10)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col >= 10")
  }

  test("Can create column between validation") {
    val result = ValidationBuilder().col("my_col").between(10, 20)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col BETWEEN 10 AND 20")
  }

  test("Can create column not between validation") {
    val result = ValidationBuilder().col("my_col").notBetween(10, 20)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col NOT BETWEEN 10 AND 20")
  }

  test("Can create column in validation") {
    val result = ValidationBuilder().col("my_col").in("open", "closed")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col IN ('open','closed')")
  }

  test("Can create column matches validation") {
    val result = ValidationBuilder().col("my_col").matches("ACC[0-9]{8}")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "REGEXP(my_col, 'ACC[0-9]{8}')")
  }

  test("Can create column not matches validation") {
    val result = ValidationBuilder().col("my_col").notMatches("ACC[0-9]{8}")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "!REGEXP(my_col, 'ACC[0-9]{8}')")
  }

  test("Can create column starts with validation") {
    val result = ValidationBuilder().col("my_col").startsWith("ACC")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "STARTSWITH(my_col, 'ACC')")
  }

  test("Can create column not starts with validation") {
    val result = ValidationBuilder().col("my_col").notStartsWith("ACC")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "!STARTSWITH(my_col, 'ACC')")
  }

  test("Can create column ends with validation") {
    val result = ValidationBuilder().col("my_col").endsWith("ACC")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "ENDSWITH(my_col, 'ACC')")
  }

  test("Can create column not ends with validation") {
    val result = ValidationBuilder().col("my_col").notEndsWith("ACC")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "!ENDSWITH(my_col, 'ACC')")
  }

  test("Can create column size validation") {
    val result = ValidationBuilder().col("my_col").size(2)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "SIZE(my_col) == 2")
  }

  test("Can create column not size validation") {
    val result = ValidationBuilder().col("my_col").notSize(5)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "SIZE(my_col) != 5")
  }

  test("Can create column less than size validation") {
    val result = ValidationBuilder().col("my_col").lessThanSize(5)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "SIZE(my_col) < 5")
  }

  test("Can create column less than or equal size validation") {
    val result = ValidationBuilder().col("my_col").lessThanOrEqualSize(5)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "SIZE(my_col) <= 5")
  }

  test("Can create column greater than size validation") {
    val result = ValidationBuilder().col("my_col").greaterThanSize(5)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "SIZE(my_col) > 5")
  }

  test("Can create column greater than or equal size validation") {
    val result = ValidationBuilder().col("my_col").greaterThanOrEqualSize(5)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "SIZE(my_col) >= 5")
  }

  test("Can create column greater luhn check validation") {
    val result = ValidationBuilder().col("my_col").luhnCheck

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "LUHN_CHECK(my_col)")
  }

  test("Can create column type validation") {
    val result = ValidationBuilder().col("my_col").hasType("double")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "TYPEOF(my_col) == 'double'")
  }

  test("Can create group by column validation") {
    val result = ValidationBuilder()
      .description("my_description")
      .errorThreshold(0.5)
      .groupBy("account_id", "year")
      .sum("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    assert(result.validation.asInstanceOf[GroupByValidation].groupByCols == Seq("account_id", "year"))
    assert(result.validation.asInstanceOf[GroupByValidation].aggCol == "amount")
    assert(result.validation.asInstanceOf[GroupByValidation].aggType == "sum")
    assert(result.validation.asInstanceOf[GroupByValidation].expr == "sum(amount) < 100")
    assert(result.validation.asInstanceOf[GroupByValidation].description.contains("my_description"))
    assert(result.validation.asInstanceOf[GroupByValidation].errorThreshold.contains(0.5))
  }

  test("Can create group by then get count column validation") {
    val result = ValidationBuilder()
      .groupBy("account_id")
      .count("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    assert(result.validation.asInstanceOf[GroupByValidation].groupByCols == Seq("account_id"))
    assert(result.validation.asInstanceOf[GroupByValidation].aggCol == "amount")
    assert(result.validation.asInstanceOf[GroupByValidation].aggType == "count")
    assert(result.validation.asInstanceOf[GroupByValidation].expr == "count(amount) < 100")
  }

  test("Can create group by then get max column validation") {
    val result = ValidationBuilder()
      .groupBy("account_id")
      .max("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    assert(result.validation.asInstanceOf[GroupByValidation].groupByCols == Seq("account_id"))
    assert(result.validation.asInstanceOf[GroupByValidation].aggCol == "amount")
    assert(result.validation.asInstanceOf[GroupByValidation].aggType == "max")
    assert(result.validation.asInstanceOf[GroupByValidation].expr == "max(amount) < 100")
  }

  test("Can create group by then get min column validation") {
    val result = ValidationBuilder()
      .groupBy("account_id")
      .min("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    assert(result.validation.asInstanceOf[GroupByValidation].groupByCols == Seq("account_id"))
    assert(result.validation.asInstanceOf[GroupByValidation].aggCol == "amount")
    assert(result.validation.asInstanceOf[GroupByValidation].aggType == "min")
    assert(result.validation.asInstanceOf[GroupByValidation].expr == "min(amount) < 100")
  }

  test("Can create group by then get average column validation") {
    val result = ValidationBuilder()
      .groupBy("account_id")
      .avg("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    assert(result.validation.asInstanceOf[GroupByValidation].groupByCols == Seq("account_id"))
    assert(result.validation.asInstanceOf[GroupByValidation].aggCol == "amount")
    assert(result.validation.asInstanceOf[GroupByValidation].aggType == "avg")
    assert(result.validation.asInstanceOf[GroupByValidation].expr == "avg(amount) < 100")
  }

  test("Can create unique column validation") {
    val result = ValidationBuilder().unique("account_id").description("my_description").errorThreshold(0.2)

    assert(result.validation.isInstanceOf[GroupByValidation])
    assert(result.validation.asInstanceOf[GroupByValidation].groupByCols == Seq("account_id"))
    assert(result.validation.asInstanceOf[GroupByValidation].aggCol == "unique")
    assert(result.validation.asInstanceOf[GroupByValidation].aggType == "count")
    assert(result.validation.asInstanceOf[GroupByValidation].expr == "count == 1")
    assert(result.validation.asInstanceOf[GroupByValidation].description.contains("my_description"))
    assert(result.validation.asInstanceOf[GroupByValidation].errorThreshold.contains(0.2))
  }

  test("Can create unique column validation with multiple columns") {
    val result = ValidationBuilder().unique("account_id", "year", "name")

    assert(result.validation.isInstanceOf[GroupByValidation])
    assert(result.validation.asInstanceOf[GroupByValidation].groupByCols == Seq("account_id", "year", "name"))
    assert(result.validation.asInstanceOf[GroupByValidation].aggCol == "unique")
    assert(result.validation.asInstanceOf[GroupByValidation].aggType == "count")
    assert(result.validation.asInstanceOf[GroupByValidation].expr == "count == 1")
  }
}
