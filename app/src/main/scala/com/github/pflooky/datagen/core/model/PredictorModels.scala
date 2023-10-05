package com.github.pflooky.datagen.core.model

import com.github.pflooky.datacaterer.api.model.Constants.{EXPRESSION, FIELD_LABEL, IS_PII}

case class FieldPrediction(fakerExpression: String, label: String, isPII: Boolean) {

  def toMap: Map[String, String] = Map(
    EXPRESSION -> fakerExpression,
    FIELD_LABEL -> label,
    IS_PII -> isPII.toString
  )
}
