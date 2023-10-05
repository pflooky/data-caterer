package com.github.pflooky.datagen.core.generator.metadata.validation

import com.github.pflooky.datacaterer.api.ValidationBuilder
import org.apache.spark.sql.types.StructField

trait ValidationPredictionCheck {

  def check(fields: Array[StructField]): List[ValidationBuilder]

  def check(field: StructField): List[ValidationBuilder]
}
