package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.model.{DataExistsWaitCondition, DataSourceValidation, ExpressionValidation, FileExistsWaitCondition, PauseWaitCondition, Validation, ValidationConfiguration, WaitCondition, WebhookWaitCondition}
import com.softwaremill.quicklens.ModifyPimp


case class ValidationConfigurationBuilder(validationConfiguration: ValidationConfiguration = ValidationConfiguration()) {
  def name(name: String): ValidationConfigurationBuilder =
    this.modify(_.validationConfiguration.name).setTo(name)

  def description(description: String): ValidationConfigurationBuilder =
    this.modify(_.validationConfiguration.description).setTo(description)

  def addDataSourceValidation(dataSourceName: String, validation: DataSourceValidationBuilder): ValidationConfigurationBuilder =
    this.modify(_.validationConfiguration.dataSources)(_ ++ Map(dataSourceName -> validation.dataSourceValidation))

  def addValidations(dataSourceName: String, options: Map[String, String], validations: ValidationBuilder*): ValidationConfigurationBuilder =
    addValidations(dataSourceName, options, WaitConditionBuilder().pause(0), validations: _*)

  def addValidations(dataSourceName: String, options: Map[String, String], waitCondition: WaitConditionBuilder, validations: ValidationBuilder*): ValidationConfigurationBuilder =
    this.modify(_.validationConfiguration.dataSources)(_ ++ Map(dataSourceName -> addValidationsToDataSource(dataSourceName, options, waitCondition, validations)))

  private def addValidationsToDataSource(
                                          dataSourceName: String,
                                          options: Map[String, String],
                                          waitCondition: WaitConditionBuilder,
                                          validations: Seq[ValidationBuilder]
                                        ): DataSourceValidation = {
    val dsValidBuilder = validationConfiguration.dataSources.get(dataSourceName) match {
      case Some(value) => DataSourceValidationBuilder(value)
      case None => DataSourceValidationBuilder()
    }
    dsValidBuilder.options(options).wait(waitCondition).validations(validations: _*).dataSourceValidation
  }

  private def addValidationToDataSource(
                                         dataSourceName: String,
                                         validation: ValidationBuilder,
                                         options: Map[String, String] = Map(),
                                         waitCondition: WaitCondition = PauseWaitCondition(),
                                       ): Map[String, DataSourceValidation] = {
    validationConfiguration.dataSources.get(dataSourceName) match {
      case Some(value) =>
        validationConfiguration.dataSources ++ Map(dataSourceName -> value.modify(_.validations)(_ ++ List(validation.validation)))
      case None =>
        Map(dataSourceName -> DataSourceValidation(options, waitCondition, List(validation.validation)))
    }
  }
}

case class DataSourceValidationBuilder(dataSourceValidation: DataSourceValidation = DataSourceValidation()) {
  def options(options: Map[String, String]): DataSourceValidationBuilder =
    this.modify(_.dataSourceValidation.options)(_ ++ options)

  def option(option: (String, String)): DataSourceValidationBuilder =
    this.modify(_.dataSourceValidation.options)(_ ++ Map(option))

  def addValidation(validation: ValidationBuilder): DataSourceValidationBuilder =
    this.modify(_.dataSourceValidation.validations)(_ ++ List(validation.validation))

  def validations(validations: ValidationBuilder*): DataSourceValidationBuilder =
    this.modify(_.dataSourceValidation.validations)(_ ++ validations.map(_.validation))

  def wait(waitCondition: WaitConditionBuilder): DataSourceValidationBuilder =
    this.modify(_.dataSourceValidation.waitCondition).setTo(waitCondition.waitCondition)
}

case class ValidationBuilder(validation: Validation = ExpressionValidation()) {

  def description(description: String): ValidationBuilder = {
    this.validation.description = Some(description)
    this
  }

  def errorThreshold(threshold: Double): ValidationBuilder = {
    this.validation.errorThreshold = Some(threshold)
    this
  }

  def expr(expr: String): ValidationBuilder = {
    val expressionValidation = ExpressionValidation(expr)
    expressionValidation.description = this.validation.description
    expressionValidation.errorThreshold = this.validation.errorThreshold
    this.modify(_.validation).setTo(expressionValidation)
  }
}

case class WaitConditionBuilder(waitCondition: WaitCondition = PauseWaitCondition()) {
  def pause(pauseInSeconds: Int): WaitConditionBuilder = this.modify(_.waitCondition).setTo(PauseWaitCondition(pauseInSeconds))

  def file(path: String): WaitConditionBuilder = this.modify(_.waitCondition).setTo(FileExistsWaitCondition(path))

  def dataExists(dataSourceName: String, options: Map[String, String], expr: String): WaitConditionBuilder =
    this.modify(_.waitCondition).setTo(DataExistsWaitCondition(dataSourceName, options, expr))

  def webhook(dataSourceName: String, url: String): WaitConditionBuilder =
    this.modify(_.waitCondition).setTo(WebhookWaitCondition(dataSourceName, url))

  def webhook(dataSourceName: String, url: String, method: String, statusCode: Int): WaitConditionBuilder =
    this.modify(_.waitCondition).setTo(WebhookWaitCondition(dataSourceName, url, method, statusCode))
}