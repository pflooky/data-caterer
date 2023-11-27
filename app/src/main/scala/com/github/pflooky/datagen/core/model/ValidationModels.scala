package com.github.pflooky.datagen.core.model

import com.github.pflooky.datacaterer.api.model.{ExpressionValidation, Validation, ValidationConfiguration}
import org.apache.spark.sql.DataFrame

case class ValidationConfigResult(
                                   name: String = "default_validation_result",
                                   description: String = "Validation result for data sources",
                                   dataSourceValidationResults: List[DataSourceValidationResult] = List()
                                 )

case class DataSourceValidationResult(
                                       dataSourceName: String = "default_data_source",
                                       options: Map[String, String] = Map(),
                                       validationResults: List[ValidationResult] = List()
                                     )

case class ValidationResult(
                             validation: Validation = ExpressionValidation(),
                             isSuccess: Boolean = true,
                             numErrors: Long = 0,
                             total: Long = 0,
                             sampleErrorValues: Option[DataFrame] = None
                           )

object ValidationResult {
  def fromValidationWithBaseResult(validation: Validation, validationResult: ValidationResult): ValidationResult = {
    ValidationResult(validation, validationResult.isSuccess, validationResult.numErrors, validationResult.total, validationResult.sampleErrorValues)
  }
}
