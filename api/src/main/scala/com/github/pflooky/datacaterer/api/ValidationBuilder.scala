package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.model.{DataExistsWaitCondition, DataSourceValidation, ExpressionValidation, FileExistsWaitCondition, PauseWaitCondition, Validation, ValidationConfiguration, WaitCondition, WebhookWaitCondition}
import com.softwaremill.quicklens.ModifyPimp

import scala.annotation.varargs


case class ValidationConfigurationBuilder(validationConfiguration: ValidationConfiguration = ValidationConfiguration()) {
  def this() = this(ValidationConfiguration())

  def name(name: String): ValidationConfigurationBuilder =
    this.modify(_.validationConfiguration.name).setTo(name)

  def description(description: String): ValidationConfigurationBuilder =
    this.modify(_.validationConfiguration.description).setTo(description)

  def addDataSourceValidation(
                               dataSourceName: String,
                               validation: DataSourceValidationBuilder
                             ): ValidationConfigurationBuilder =
    this.modify(_.validationConfiguration.dataSources)(_ ++ Map(dataSourceName -> validation.dataSourceValidation))

  def addDataSourceValidation(
                               dataSourceName: String,
                               validation: DataSourceValidation
                             ): ValidationConfigurationBuilder =
    this.modify(_.validationConfiguration.dataSources)(_ ++ Map(dataSourceName -> validation))

  def addValidations(
                      dataSourceName: String,
                      validations: Seq[ValidationBuilder]
                    ): ValidationConfigurationBuilder =
    this.modify(_.validationConfiguration.dataSources)(_ ++ Map(dataSourceName -> addValidationsToDataSource(dataSourceName, validations)))

  @varargs def addValidations(
                               dataSourceName: String,
                               options: Map[String, String],
                               validations: ValidationBuilder*
                             ): ValidationConfigurationBuilder =
    addValidations(dataSourceName, options, WaitConditionBuilder().pause(0), validations: _*)

  @varargs def addValidations(
                               dataSourceName: String,
                               options: Map[String, String],
                               waitCondition: WaitConditionBuilder,
                               validationBuilders: ValidationBuilder*
                             ): ValidationConfigurationBuilder =
    this.modify(_.validationConfiguration.dataSources)(_ ++
      Map(dataSourceName -> addValidationsToDataSource(dataSourceName, options, waitCondition, validationBuilders))
    )

  private def addValidationsToDataSource(
                                          dataSourceName: String,
                                          options: Map[String, String],
                                          waitCondition: WaitConditionBuilder,
                                          validation: Seq[ValidationBuilder]
                                        ): DataSourceValidation = {
    val dsValidBuilder = getDsBuilder(dataSourceName)
    dsValidBuilder.options(options).wait(waitCondition).validations(validation: _*).dataSourceValidation
  }

  private def addValidationsToDataSource(
                                          dataSourceName: String,
                                          validations: Seq[ValidationBuilder]
                                        ): DataSourceValidation = {
    val dsValidBuilder = getDsBuilder(dataSourceName)
    dsValidBuilder.validations(validations: _*).dataSourceValidation
  }

  private def getDsBuilder(dataSourceName: String) = {
    val dsValidBuilder = validationConfiguration.dataSources.get(dataSourceName) match {
      case Some(value) => DataSourceValidationBuilder(value)
      case None => DataSourceValidationBuilder()
    }
    dsValidBuilder
  }
}

case class DataSourceValidationBuilder(dataSourceValidation: DataSourceValidation = DataSourceValidation()) {
  def this() = this(DataSourceValidation())

  def options(options: Map[String, String]): DataSourceValidationBuilder =
    this.modify(_.dataSourceValidation.options)(_ ++ options)

  def option(option: (String, String)): DataSourceValidationBuilder =
    this.modify(_.dataSourceValidation.options)(_ ++ Map(option))

  @varargs def validations(validations: ValidationBuilder*): DataSourceValidationBuilder =
    this.modify(_.dataSourceValidation.validations)(_ ++ validations)

  def wait(waitCondition: WaitConditionBuilder): DataSourceValidationBuilder =
    this.modify(_.dataSourceValidation.waitCondition).setTo(waitCondition.waitCondition)

  def wait(waitCondition: WaitCondition): DataSourceValidationBuilder =
    this.modify(_.dataSourceValidation.waitCondition).setTo(waitCondition)
}

case class ValidationBuilder(validation: Validation = ExpressionValidation()) {
  def this() = this(ExpressionValidation())

  def description(description: String): ValidationBuilder = {
    this.validation.description = Some(description)
    this
  }

  /**
   * Define the number of records or percentage of records that do not meet the validation rule before marking the validation
   * as failed. If no error threshold is defined, any failures will mark the whole validation as failed.<br>
   * For example, if there are 10 records and 4 have failed:<br>
   * {{{errorThreshold(2) #marked as failed as more than 2 records have failed}}}
   * {{{errorThreshold(0.1) #marked as failed as more than 10% of records have failed}}}
   * {{{errorThreshold(4) #marked as success as less than or equal to 4 records have failed}}}
   * {{{errorThreshold(0.4) #marked as success as less than or equal to 40% of records have failed}}}
   *
   * @param threshold Number or percentage of failed records which is acceptable before marking as failed
   * @return ValidationBuilder
   */
  def errorThreshold(threshold: Double): ValidationBuilder = {
    this.validation.errorThreshold = Some(threshold)
    this
  }

  /**
   * SQL expression used to check if data is adhering to specified condition. Return result from SQL expression is
   * required to be boolean
   *
   * @param expr SQL expression which returns a boolean
   * @return ValidationBuilder
   */
  def expr(expr: String): ValidationBuilder = {
    val expressionValidation = ExpressionValidation(expr)
    expressionValidation.description = this.validation.description
    expressionValidation.errorThreshold = this.validation.errorThreshold
    this.modify(_.validation).setTo(expressionValidation)
  }
}

case class WaitConditionBuilder(waitCondition: WaitCondition = PauseWaitCondition()) {
  def this() = this(PauseWaitCondition())

  def pause(pauseInSeconds: Int): WaitConditionBuilder = this.modify(_.waitCondition).setTo(PauseWaitCondition(pauseInSeconds))

  def file(path: String): WaitConditionBuilder = this.modify(_.waitCondition).setTo(FileExistsWaitCondition(path))

  def dataExists(dataSourceName: String, options: Map[String, String], expr: String): WaitConditionBuilder =
    this.modify(_.waitCondition).setTo(DataExistsWaitCondition(dataSourceName, options, expr))

  def webhook(url: String): WaitConditionBuilder =
    webhook("tmp_http_data_source", url)

  @varargs def webhook(url: String, method: String, statusCodes: Int*): WaitConditionBuilder =
    webhook("tmp_http_data_source", url, method, statusCodes: _*)

  def webhook(dataSourceName: String, url: String): WaitConditionBuilder =
    this.modify(_.waitCondition).setTo(WebhookWaitCondition(dataSourceName, url))

  @varargs def webhook(dataSourceName: String, url: String, method: String, statusCode: Int*): WaitConditionBuilder =
    this.modify(_.waitCondition).setTo(WebhookWaitCondition(dataSourceName, url, method, statusCode.toList))
}