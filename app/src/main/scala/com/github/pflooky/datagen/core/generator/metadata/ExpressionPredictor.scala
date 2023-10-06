package com.github.pflooky.datagen.core.generator.metadata

import com.github.pflooky.datacaterer.api.model.Constants.{LABEL_ADDRESS, LABEL_APP, LABEL_FOOD, LABEL_INTERNET, LABEL_JOB, LABEL_MONEY, LABEL_NAME, LABEL_NATION, LABEL_PHONE, LABEL_RELATIONSHIP, LABEL_USERNAME, LABEL_WEATHER}
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
        case "firstname" => Some(FieldPrediction("Name.firstname", LABEL_NAME, true))
        case "lastname" => Some(FieldPrediction("Name.lastname", LABEL_NAME, true))
        case "username" => Some(FieldPrediction("Name.username", LABEL_USERNAME, true))
        case "name" | "fullname" => Some(FieldPrediction("Name.name", LABEL_NAME, true))
        case "city" => Some(FieldPrediction("Address.city", LABEL_ADDRESS, false))
        case "country" => Some(FieldPrediction("Address.country", LABEL_ADDRESS, false))
        case "countrycode" => Some(FieldPrediction("Address.countryCode", LABEL_ADDRESS, false))
        case "nationality" => Some(FieldPrediction("Nation.nationality", LABEL_NATION, false))
        case "language" => Some(FieldPrediction("Nation.language", LABEL_NATION, false))
        case "capital" | "capitalcity" => Some(FieldPrediction("Nation.capitalCity", LABEL_NATION, false))
        case "version" => Some(FieldPrediction("App.version", LABEL_APP, false))
        case "paymentmethod" => Some(FieldPrediction("Subscription.paymentMethods", LABEL_MONEY, false))
        case "macaddress" => Some(FieldPrediction("Internet.macAddress", LABEL_INTERNET, true))
        case "currency" => Some(FieldPrediction("Money.currency", LABEL_MONEY, false))
        case "currencycode" => Some(FieldPrediction("Money.currencyCode", LABEL_MONEY, false))
        case "creditcard" => Some(FieldPrediction("Finance.creditCard", LABEL_MONEY, true))
        case "food" | "dish" => Some(FieldPrediction("Food.dish", LABEL_FOOD, false))
        case "ingredient" => Some(FieldPrediction("Food.ingredient", LABEL_FOOD, false))
        case "jobfield" => Some(FieldPrediction("Job.field", LABEL_JOB, false))
        case "jobposition" => Some(FieldPrediction("Job.position", LABEL_JOB, false))
        case "jobtitle" => Some(FieldPrediction("Job.title", LABEL_JOB, false))
        case "relationship" => Some(FieldPrediction("Relationship.any", LABEL_RELATIONSHIP, false))
        case "weather" => Some(FieldPrediction("Weather.description", LABEL_WEATHER, false))
        case "cellphone" | "mobilephone" | "homephone" | "phone" => Some(FieldPrediction("PhoneNumber.cellPhone", LABEL_PHONE, true))
        case x if x.contains("email") => Some(FieldPrediction("Internet.emailAddress", LABEL_INTERNET, true))
        case x if x.contains("ipv4") => Some(FieldPrediction("Internet.ipV4Address", LABEL_INTERNET, true))
        case x if x.contains("ipv6") => Some(FieldPrediction("Internet.ipV6Address", LABEL_INTERNET, true))
        case x if x.contains("address") => Some(FieldPrediction("Address.fullAddress", LABEL_ADDRESS, true))
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
