package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.model.{ExpressionValidation, PauseWaitCondition}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

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

}
