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

object ValidationConfigurationHelper {
  /**
   * A few different scenarios:
   * - user defined validations, no generated validations
   * - user defined validations, generated validations
   * - user defined validation for 1 data source, generated validations for 2 data sources
   *
   * @param userValidationConf      User defined validation configuration
   * @param generatedValidationConf Generated validation configuration from metadata sources or from generated suggestions
   * @return Merged ValidationConfiguration
   */
  def merge(userValidationConf: List[ValidationConfiguration], generatedValidationConf: ValidationConfiguration): ValidationConfiguration = {
    val userDataSourceValidations = userValidationConf.flatMap(_.dataSources)
    val genDataSourceValidations = generatedValidationConf.dataSources

    val mergedUserDataSourceValidations = userDataSourceValidations.map(userDsValid => {
      val currentUserDsValid = userDsValid._2
      val combinedDataSourceValidations = genDataSourceValidations.get(userDsValid._1)
        .map(dsv2 => {
          dsv2.map(genV => {
            currentUserDsValid.find(_.options == genV.options)
              .map(matchUserDef =>
                matchUserDef.copy(validations = genV.validations ++ matchUserDef.validations)
              )
              .getOrElse(genV)
          })
        }).getOrElse(currentUserDsValid)
      (userDsValid._1, combinedDataSourceValidations)
    }).toMap

    //data source from generated not in user
    val genDsValidationNotInUser = genDataSourceValidations.filter(genDs => !userDataSourceValidations.exists(_._1 == genDs._1))
    val allValidations = mergedUserDataSourceValidations ++ genDsValidationNotInUser
    userValidationConf.head.copy(dataSources = allValidations)
  }
}

