package com.github.pflooky.datacaterer.api.parser

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase
import com.fasterxml.jackson.databind.{DatabindContext, JavaType, JsonSerializer, SerializerProvider}
import com.github.pflooky.datacaterer.api.ValidationBuilder
import com.github.pflooky.datacaterer.api.model.{ExpressionValidation, GroupByValidation}

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

class ValidationBuilderSerializer extends JsonSerializer[ValidationBuilder] {
  override def serialize(value: ValidationBuilder, gen: JsonGenerator, serializers: SerializerProvider): Unit = {
    val validation = value.validation
    gen.writeStartObject()
    validation match {
      case ExpressionValidation(expr) =>
        gen.writeStringField("expr", expr)
      case GroupByValidation(groupByCols, aggCol, aggType, expr) =>
        gen.writeArrayFieldStart("groupByCols")
        groupByCols.foreach(gen.writeObject)
        gen.writeEndArray()
        gen.writeStringField("aggCol", aggCol)
        gen.writeStringField("aggType", aggType)
        gen.writeStringField("expr", expr)
      case _ =>
    }
    gen.writeEndObject()
  }
}
