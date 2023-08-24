package com.github.pflooky.datagen.core.parser

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase
import com.fasterxml.jackson.databind.{DatabindContext, JavaType}
import com.github.pflooky.datagen.core.model.{ExpressionValidation, ValidationConfiguration}
import org.apache.spark.sql.SparkSession

object ValidationParser {

  def parseValidation(validationFolderPath: String)(implicit sparkSession: SparkSession): Array[ValidationConfiguration] = {
    YamlFileParser.parseFiles[ValidationConfiguration](validationFolderPath)
  }
}

class ValidationIdResolver extends TypeIdResolverBase {
  private var superType: JavaType = null

  override def init(bt: JavaType): Unit = {
    superType = bt
  }

  override def idFromValue(value: Any): String = null

  override def idFromValueAndType(value: Any, suggestedType: Class[_]): String = null

  override def getMechanism: Id = null

  override def typeFromId(context: DatabindContext, id: String): JavaType = {
    val subType = id match {
      case x => classOf[ExpressionValidation]
      case _ => throw new RuntimeException()
    }
    context.constructSpecializedType(superType, subType)
  }
}
