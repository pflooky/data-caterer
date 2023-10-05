package com.github.pflooky.datagen.core.generator.metadata

import com.github.pflooky.datacaterer.api.model.Constants.{LABEL_ADDRESS, LABEL_APP, LABEL_FOOD, LABEL_INTERNET, LABEL_JOB, LABEL_MONEY, LABEL_NAME, LABEL_NATION, LABEL_PHONE, LABEL_RELATIONSHIP, LABEL_USERNAME, LABEL_WEATHER}
import org.apache.spark.sql.types.{StringType, StructField}
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

  test("When given field with name first_name, use first name expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("first_name"))

    assert(result.get.fakerExpression == "#{Name.firstname}")
    assert(result.get.label == LABEL_NAME)
    assert(result.get.isPII)
  }

  test("When given field with name last_name, use last name expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("last_name"))

    assert(result.get.fakerExpression == "#{Name.lastname}")
    assert(result.get.label == LABEL_NAME)
    assert(result.get.isPII)
  }

  test("When given field with name username, use username expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("username"))

    assert(result.get.fakerExpression == "#{Name.username}")
    assert(result.get.label == LABEL_USERNAME)
    assert(result.get.isPII)
  }

  test("When given field called name, use name expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("name"))

    assert(result.get.fakerExpression == "#{Name.name}")
    assert(result.get.label == LABEL_NAME)
    assert(result.get.isPII)
  }

  test("When given field called full_name, use name expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("full_name"))

    assert(result.get.fakerExpression == "#{Name.name}")
    assert(result.get.label == LABEL_NAME)
    assert(result.get.isPII)
  }

  test("When given field called city, use city expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("city"))

    assert(result.get.fakerExpression == "#{Address.city}")
    assert(result.get.label == LABEL_ADDRESS)
    assert(!result.get.isPII)
  }

  test("When given field called country, use country expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("country"))

    assert(result.get.fakerExpression == "#{Address.country}")
    assert(result.get.label == LABEL_ADDRESS)
    assert(!result.get.isPII)
  }

  test("When given field called country_code, use country code expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("country_code"))

    assert(result.get.fakerExpression == "#{Address.countryCode}")
    assert(result.get.label == LABEL_ADDRESS)
    assert(!result.get.isPII)
  }

  test("When given field called nationality, use nationality expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("nationality"))

    assert(result.get.fakerExpression == "#{Nation.nationality}")
    assert(result.get.label == LABEL_NATION)
    assert(!result.get.isPII)
  }

  test("When given field called capital_city, use capital city expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("capital_city"))

    assert(result.get.fakerExpression == "#{Nation.capitalCity}")
    assert(result.get.label == LABEL_NATION)
    assert(!result.get.isPII)
  }

  test("When given field called capital, use capital city expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("capital"))

    assert(result.get.fakerExpression == "#{Nation.capitalCity}")
    assert(result.get.label == LABEL_NATION)
    assert(!result.get.isPII)
  }

  test("When given field called address, use full address expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("address"))

    assert(result.get.fakerExpression == "#{Address.fullAddress}")
    assert(result.get.label == LABEL_ADDRESS)
    assert(result.get.isPII)
  }

  test("When given field called customer_address, use full address expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("customer_address"))

    assert(result.get.fakerExpression == "#{Address.fullAddress}")
    assert(result.get.label == LABEL_ADDRESS)
    assert(result.get.isPII)
  }

  test("When given field called version, use version expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("version"))

    assert(result.get.fakerExpression == "#{App.version}")
    assert(result.get.label == LABEL_APP)
    assert(!result.get.isPII)
  }

  test("When given field called payment_method, use payment method expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("payment_method"))

    assert(result.get.fakerExpression == "#{Subscription.paymentMethods}")
    assert(result.get.label == LABEL_MONEY)
    assert(!result.get.isPII)
  }

  test("When given field with name email_address, use email expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("email_address"))

    assert(result.get.fakerExpression == "#{Internet.emailAddress}")
    assert(result.get.label == LABEL_INTERNET)
    assert(result.get.isPII)
  }

  test("When given field with name containing email, use email expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("customer_email"))

    assert(result.get.fakerExpression == "#{Internet.emailAddress}")
    assert(result.get.label == LABEL_INTERNET)
    assert(result.get.isPII)
  }

  test("When given field called mac_address, use mac address expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("mac_address"))

    assert(result.get.fakerExpression == "#{Internet.macAddress}")
    assert(result.get.label == LABEL_INTERNET)
    assert(result.get.isPII)
  }

  test("When given field called ipv4, use ipv4 address expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("ipv4"))

    assert(result.get.fakerExpression == "#{Internet.ipV4Address}")
    assert(result.get.label == LABEL_INTERNET)
    assert(result.get.isPII)
  }

  test("When given field called ipv4_address, use ipv4 address expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("ipv4_address"))

    assert(result.get.fakerExpression == "#{Internet.ipV4Address}")
    assert(result.get.label == LABEL_INTERNET)
    assert(result.get.isPII)
  }

  test("When given field called ipv6, use ipv6 address expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("ipv6"))

    assert(result.get.fakerExpression == "#{Internet.ipV6Address}")
    assert(result.get.label == LABEL_INTERNET)
    assert(result.get.isPII)
  }

  test("When given field called ipv6_address, use ipv6 address expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("ipv6_address"))

    assert(result.get.fakerExpression == "#{Internet.ipV6Address}")
    assert(result.get.label == LABEL_INTERNET)
    assert(result.get.isPII)
  }

  test("When given field called currency, use currency expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("currency"))

    assert(result.get.fakerExpression == "#{Money.currency}")
    assert(result.get.label == LABEL_MONEY)
    assert(!result.get.isPII)
  }

  test("When given field called currency_code, use currency code expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("currency_code"))

    assert(result.get.fakerExpression == "#{Money.currencyCode}")
    assert(result.get.label == LABEL_MONEY)
    assert(!result.get.isPII)
  }

  test("When given field called credit_card, use credit card expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("credit_card"))

    assert(result.get.fakerExpression == "#{Finance.creditCard}")
    assert(result.get.label == LABEL_MONEY)
    assert(result.get.isPII)
  }

  test("When given field called food, use dish expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("food"))

    assert(result.get.fakerExpression == "#{Food.dish}")
    assert(result.get.label == LABEL_FOOD)
    assert(!result.get.isPII)
  }

  test("When given field called dish, use dish expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("dish"))

    assert(result.get.fakerExpression == "#{Food.dish}")
    assert(result.get.label == LABEL_FOOD)
    assert(!result.get.isPII)
  }

  test("When given field called ingredient, use ingredient expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("ingredient"))

    assert(result.get.fakerExpression == "#{Food.ingredient}")
    assert(result.get.label == LABEL_FOOD)
    assert(!result.get.isPII)
  }

  test("When given field called job_field, use job field expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("job_field"))

    assert(result.get.fakerExpression == "#{Job.field}")
    assert(result.get.label == LABEL_JOB)
    assert(!result.get.isPII)
  }

  test("When given field called job_position, use job position expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("job_position"))

    assert(result.get.fakerExpression == "#{Job.position}")
    assert(result.get.label == LABEL_JOB)
    assert(!result.get.isPII)
  }

  test("When given field called job_title, use job title expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("job_title"))

    assert(result.get.fakerExpression == "#{Job.title}")
    assert(result.get.label == LABEL_JOB)
    assert(!result.get.isPII)
  }

  test("When given field called relationship, use relationship expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("relationship"))

    assert(result.get.fakerExpression == "#{Relationship.any}")
    assert(result.get.label == LABEL_RELATIONSHIP)
    assert(!result.get.isPII)
  }

  test("When given field called weather, use weather description expression") {
    val result = ExpressionPredictor.getFakerExpressionAndLabel(field("weather"))

    assert(result.get.fakerExpression == "#{Weather.description}")
    assert(result.get.label == LABEL_WEATHER)
    assert(!result.get.isPII)
  }

  test("When given field name that contains phone, use phone number expression") {
    val results = List(
      ExpressionPredictor.getFakerExpressionAndLabel(field("cell_phone")),
      ExpressionPredictor.getFakerExpressionAndLabel(field("mobile_phone")),
      ExpressionPredictor.getFakerExpressionAndLabel(field("home_phone")),
      ExpressionPredictor.getFakerExpressionAndLabel(field("HomePhone")),
      ExpressionPredictor.getFakerExpressionAndLabel(field("Homephone")),
      ExpressionPredictor.getFakerExpressionAndLabel(field("home phone")),
      ExpressionPredictor.getFakerExpressionAndLabel(field("phone")),
    )

    results.foreach(result => {
      assert(result.get.fakerExpression == "#{PhoneNumber.cellPhone}")
      assert(result.get.label == LABEL_PHONE)
      assert(result.get.isPII)
    })
  }

  private def field(name: String): StructField = StructField(name, StringType)
}
