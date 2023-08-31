package com.github.pflooky.datacaterer.api.parser

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase
import com.fasterxml.jackson.databind.{DatabindContext, JavaType}
import com.github.pflooky.datacaterer.api.model.ExpressionValidation

class ValidationIdResolver extends TypeIdResolverBase {
  private var superType: JavaType = null

  override def init(bt: JavaType): Unit = {
    superType = bt
  }

  override def idFromValue(value: Any): String = null

  override def idFromValueAndType(value: Any, suggestedType: Class[_]): String = null

  override def getMechanism: Id = null

  override def typeFromId(context: DatabindContext, id: String): JavaType = {
    val subType = classOf[ExpressionValidation]
    context.constructSpecializedType(superType, subType)
  }
}

