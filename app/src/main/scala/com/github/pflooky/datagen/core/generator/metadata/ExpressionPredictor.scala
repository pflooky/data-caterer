package com.github.pflooky.datagen.core.generator.metadata

import com.github.pflooky.datacaterer.api.model.Constants.{FAKER_EXPR_ADDRESS, FAKER_EXPR_APP_VERSION, FAKER_EXPR_CAPITAL, FAKER_EXPR_CITY, FAKER_EXPR_COUNTRY, FAKER_EXPR_COUNTRY_CODE, FAKER_EXPR_CREDIT_CARD, FAKER_EXPR_CURRENCY, FAKER_EXPR_CURRENCY_CODE, FAKER_EXPR_EMAIL, FAKER_EXPR_FIRST_NAME, FAKER_EXPR_FOOD, FAKER_EXPR_FOOD_INGREDIENT, FAKER_EXPR_IPV4, FAKER_EXPR_IPV6, FAKER_EXPR_JOB_FIELD, FAKER_EXPR_JOB_POSITION, FAKER_EXPR_JOB_TITLE, FAKER_EXPR_LANGUAGE, FAKER_EXPR_LAST_NAME, FAKER_EXPR_MAC_ADDRESS, FAKER_EXPR_NAME, FAKER_EXPR_NATIONALITY, FAKER_EXPR_PAYMENT_METHODS, FAKER_EXPR_PHONE, FAKER_EXPR_RELATIONSHIP, FAKER_EXPR_USERNAME, FAKER_EXPR_WEATHER, LABEL_ADDRESS, LABEL_APP, LABEL_FOOD, LABEL_INTERNET, LABEL_JOB, LABEL_MONEY, LABEL_NAME, LABEL_NATION, LABEL_PHONE, LABEL_RELATIONSHIP, LABEL_USERNAME, LABEL_WEATHER}
import com.github.pflooky.datagen.core.model.FieldPrediction
import net.datafaker.providers.base.AbstractProvider
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StringType, StructField}
import org.reflections.Reflections

import scala.collection.JavaConverters.asScalaSetConverter

object ExpressionPredictor {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val DEFAULT_METHODS = List("toString")

  def getAllFakerExpressionTypes: List[String] = {
    val reflections = new Reflections("net.datafaker.providers")
    val allFakerDataProviders = reflections.getSubTypesOf(classOf[AbstractProvider[_]]).asScala
    allFakerDataProviders.flatMap(c => {
      c.getMethods
        .filter(m => isStringType(m.getReturnType) && !DEFAULT_METHODS.contains(m.getName))
        .map(m => s"${c.getSimpleName}.${m.getName}")
    }).toList
  }

  def tryGetFieldPrediction(structField: StructField): Option[FieldPrediction] = {
    if (structField.dataType == StringType) {
      val cleanFieldName = structField.name.toLowerCase.replaceAll("[^a-z0-9]", "")
      val optExpression = cleanFieldName match {
        case "firstname" => Some(FieldPrediction(FAKER_EXPR_FIRST_NAME, LABEL_NAME, true))
        case "lastname" => Some(FieldPrediction(FAKER_EXPR_LAST_NAME, LABEL_NAME, true))
        case "username" => Some(FieldPrediction(FAKER_EXPR_USERNAME, LABEL_USERNAME, true))
        case "name" | "fullname" => Some(FieldPrediction(FAKER_EXPR_NAME, LABEL_NAME, true))
        case "city" => Some(FieldPrediction(FAKER_EXPR_CITY, LABEL_ADDRESS, false))
        case "country" => Some(FieldPrediction(FAKER_EXPR_COUNTRY, LABEL_ADDRESS, false))
        case "countrycode" => Some(FieldPrediction(FAKER_EXPR_COUNTRY_CODE, LABEL_ADDRESS, false))
        case "nationality" => Some(FieldPrediction(FAKER_EXPR_NATIONALITY, LABEL_NATION, false))
        case "language" => Some(FieldPrediction(FAKER_EXPR_LANGUAGE, LABEL_NATION, false))
        case "capital" | "capitalcity" => Some(FieldPrediction(FAKER_EXPR_CAPITAL, LABEL_NATION, false))
        case "version" => Some(FieldPrediction(FAKER_EXPR_APP_VERSION, LABEL_APP, false))
        case "paymentmethod" => Some(FieldPrediction(FAKER_EXPR_PAYMENT_METHODS, LABEL_MONEY, false))
        case "macaddress" | "macaddr" => Some(FieldPrediction(FAKER_EXPR_MAC_ADDRESS, LABEL_INTERNET, true))
        case "currency" => Some(FieldPrediction(FAKER_EXPR_CURRENCY, LABEL_MONEY, false))
        case "currencycode" => Some(FieldPrediction(FAKER_EXPR_CURRENCY_CODE, LABEL_MONEY, false))
        case "creditcard" => Some(FieldPrediction(FAKER_EXPR_CREDIT_CARD, LABEL_MONEY, true))
        case "food" | "dish" => Some(FieldPrediction(FAKER_EXPR_FOOD, LABEL_FOOD, false))
        case "ingredient" => Some(FieldPrediction(FAKER_EXPR_FOOD_INGREDIENT, LABEL_FOOD, false))
        case "jobfield" => Some(FieldPrediction(FAKER_EXPR_JOB_FIELD, LABEL_JOB, false))
        case "jobposition" => Some(FieldPrediction(FAKER_EXPR_JOB_POSITION, LABEL_JOB, false))
        case "jobtitle" => Some(FieldPrediction(FAKER_EXPR_JOB_TITLE, LABEL_JOB, false))
        case "relationship" => Some(FieldPrediction(FAKER_EXPR_RELATIONSHIP, LABEL_RELATIONSHIP, false))
        case "weather" => Some(FieldPrediction(FAKER_EXPR_WEATHER, LABEL_WEATHER, false))
        case "cellphone" | "mobilephone" | "homephone" | "phone" => Some(FieldPrediction(FAKER_EXPR_PHONE, LABEL_PHONE, true))
        case x if x.contains("email") => Some(FieldPrediction(FAKER_EXPR_EMAIL, LABEL_INTERNET, true))
        case x if x.contains("ipv4") => Some(FieldPrediction(FAKER_EXPR_IPV4, LABEL_INTERNET, true))
        case x if x.contains("ipv6") => Some(FieldPrediction(FAKER_EXPR_IPV6, LABEL_INTERNET, true))
        case x if x.contains("address") => Some(FieldPrediction(FAKER_EXPR_ADDRESS, LABEL_ADDRESS, true))
        case _ => None
      }
      if (optExpression.isDefined) {
        LOGGER.debug(s"Identified column as a faker expression, column-name=${structField.name}, expression=${optExpression.get}")
      }
      optExpression.map(e => e.copy(fakerExpression = s"#{${e.fakerExpression}}"))
    } else {
      None
    }
  }

  private def isStringType(clazz: Class[_]): Boolean = {
    clazz.getSimpleName match {
      case "String" => true
      case _ => false
    }
  }

}
