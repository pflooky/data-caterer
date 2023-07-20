package com.github.pflooky.datagen.core.generator.plan

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

  def getFakerExpression(structField: StructField): Option[String] = {
    if (structField.dataType == StringType) {
      val cleanFieldName = structField.name.toLowerCase.replaceAll("[^a-z]", "")
      val optExpression = cleanFieldName match {
        case "firstname" => Some("Name.firstname")
        case "lastname" => Some("Name.lastname")
        case "username" => Some("Name.username")
        case "name" => Some("Name.name")
        case "city" => Some("Address.city")
        case "country" => Some("Address.country")
        case "countrycode" => Some("Address.countryCode")
        case "nationality" => Some("Nation.nationality")
        case "language" => Some("Nation.language")
        case "capital" | "capitalcity" => Some("Nation.capitalCity")
        case "address" => Some("Address.fullAddress")
        case "version" => Some("App.version")
        case "paymentmethod" => Some("Subscription.paymentMethods")
        case "email" | "emailaddress" => Some("Internet.emailAddress")
        case "macaddress" => Some("Internet.macAddress")
        case "ipv4" => Some("Internet.ipV4Address")
        case "ipv6" => Some("Internet.ipV6Address")
        case "currency" => Some("Money.currency")
        case "currencycode" => Some("Money.currencyCode")
        case "creditcard" => Some("Finance.creditCard")
        case "food" | "dish" => Some("Food.dish")
        case "ingredient" => Some("Food.ingredient")
        case "jobfield" => Some("Job.field")
        case "jobposition" => Some("Job.position")
        case "jobtitle" => Some("Job.title")
        case "relationship" => Some("Relationship.any")
        case "weather" => Some("Weather.description")
        case _ => None
      }
      if (optExpression.isDefined) {
        LOGGER.debug(s"Identified column as a faker expression, column-name=${structField.name}, expression=${optExpression.get}")
      }
      optExpression.map(e => s"#{$e}")
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
