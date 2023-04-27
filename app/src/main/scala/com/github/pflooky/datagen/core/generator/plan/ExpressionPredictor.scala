package com.github.pflooky.datagen.core.generator.plan

import net.datafaker.providers.base.AbstractProvider
import org.apache.spark.sql.types.{StringType, StructField}
import org.reflections.Reflections

import scala.collection.JavaConverters.asScalaSetConverter

class ExpressionPredictor {

  private val defaultMethods = List("toString")

  def getAllFakerExpressionTypes: List[String] = {
    val reflections = new Reflections("net.datafaker.providers")
    val allFakerDataProviders = reflections.getSubTypesOf(classOf[AbstractProvider[_]]).asScala
    allFakerDataProviders.flatMap(c => {
      c.getMethods
        .filter(m => isStringType(m.getReturnType) && !defaultMethods.contains(m.getName))
        .map(m => s"${c.getSimpleName}.${m.getName}")
    }).toList
  }

  def getFakerExpression(structField: StructField): Option[String] = {
    if (structField.dataType == StringType) {
      val cleanFieldName = structField.name.toLowerCase.replaceAll("![a-z]", "")
      Some("")
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
