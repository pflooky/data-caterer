package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.model.Constants.{AGGREGATION_AVG, AGGREGATION_COUNT, AGGREGATION_MAX, AGGREGATION_MIN, AGGREGATION_SUM}
import com.github.pflooky.datacaterer.api.model.{DataExistsWaitCondition, DataSourceValidation, ExpressionValidation, FileExistsWaitCondition, GroupByValidation, PauseWaitCondition, Validation, ValidationConfiguration, WaitCondition, WebhookWaitCondition}
import com.softwaremill.quicklens.ModifyPimp

import java.sql.{Date, Timestamp}
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
   * required to be boolean. Can use any columns in the validation logic.
   *
   * For example,
   * {{{validation.expr("CASE WHEN status == 'open' THEN balance > 0 ELSE balance == 0 END}}}
   *
   * @param expr SQL expression which returns a boolean
   * @return ValidationBuilder
   * @see <a href="https://spark.apache.org/docs/latest/api/sql/">SQL expressions</a>
   */
  def expr(expr: String): ValidationBuilder = {
    validation match {
      case GroupByValidation(grpCols, aggCol, aggType, _) =>
        val grpWithExpr = GroupByValidation(grpCols, aggCol, aggType, expr)
        grpWithExpr.description = this.validation.description
        grpWithExpr.errorThreshold = this.validation.errorThreshold
        this.modify(_.validation).setTo(grpWithExpr)
      case expressionValidation: ExpressionValidation =>
        val withExpr = expressionValidation.modify(_.expr).setTo(expr)
        withExpr.description = this.validation.description
        withExpr.errorThreshold = this.validation.errorThreshold
        this.modify(_.validation).setTo(withExpr)
    }
  }

  /**
   * Define a column validation that can cover validations for any type of data.
   *
   * @param column Name of the column to run validation against
   * @return ColumnValidationBuilder
   */
  def col(column: String): ColumnValidationBuilder = {
    ColumnValidationBuilder(column, this)
  }

  /**
   * Define columns to group by, so that validation can be run on grouped by dataset
   *
   * @param columns Name of the column to run validation against
   * @return ColumnValidationBuilder
   */
  @varargs def groupBy(columns: String*): GroupByValidationBuilder = {
    GroupByValidationBuilder(this, ColumnValidationBuilder(), columns)
  }

}

case class ColumnValidationBuilder(column: String = "", validationBuilder: ValidationBuilder = ValidationBuilder()) {
  def this() = this("", ValidationBuilder())

  def isEqual(value: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column == ${colValueToString(value)}")
  }

  def isNotEqual(value: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column != ${colValueToString(value)}")
  }

  def isNull: ValidationBuilder = {
    validationBuilder.expr(s"ISNULL($column)")
  }

  def isNotNull: ValidationBuilder = {
    validationBuilder.expr(s"ISNOTNULL($column)")
  }

  def contains(value: String): ValidationBuilder = {
    validationBuilder.expr(s"CONTAINS($column, '$value')")
  }

  def notContains(value: String): ValidationBuilder = {
    validationBuilder.expr(s"!CONTAINS($column, '$value')")
  }

  def lessThan(value: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column < ${colValueToString(value)}")
  }

  def lessThanOrEqual(value: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column <= ${colValueToString(value)}")
  }

  def greaterThan(value: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column > ${colValueToString(value)}")
  }

  def greaterThanOrEqual(value: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column >= ${colValueToString(value)}")
  }

  def between(minValue: Any, maxValue: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column BETWEEN ${colValueToString(minValue)} AND ${colValueToString(maxValue)}")
  }

  def notBetween(minValue: Any, maxValue: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column NOT BETWEEN ${colValueToString(minValue)} AND ${colValueToString(maxValue)}")
  }

  @varargs def in(values: Any*): ValidationBuilder = {
    validationBuilder.expr(s"$column IN (${values.map(colValueToString).mkString(",")})")
  }

  def matches(regex: String): ValidationBuilder = {
    validationBuilder.expr(s"REGEXP($column, '$regex')")
  }

  def notMatches(regex: String): ValidationBuilder = {
    validationBuilder.expr(s"!REGEXP($column, '$regex')")
  }

  def startsWith(value: String): ValidationBuilder = {
    validationBuilder.expr(s"STARTSWITH($column, '$value')")
  }

  def notStartsWith(value: String): ValidationBuilder = {
    validationBuilder.expr(s"!STARTSWITH($column, '$value')")
  }

  def endsWith(value: String): ValidationBuilder = {
    validationBuilder.expr(s"ENDSWITH($column, '$value')")
  }

  def notEndsWith(value: String): ValidationBuilder = {
    validationBuilder.expr(s"!ENDSWITH($column, '$value')")
  }

  def size(size: Int): ValidationBuilder = {
    validationBuilder.expr(s"SIZE($column) == $size")
  }

  def notSize(size: Int): ValidationBuilder = {
    validationBuilder.expr(s"SIZE($column) != $size")
  }

  def lessThanSize(size: Int): ValidationBuilder = {
    validationBuilder.expr(s"SIZE($column) < $size")
  }

  def lessThanOrEqualSize(size: Int): ValidationBuilder = {
    validationBuilder.expr(s"SIZE($column) <= $size")
  }

  def greaterThanSize(size: Int): ValidationBuilder = {
    validationBuilder.expr(s"SIZE($column) > $size")
  }

  def greaterThanOrEqualSize(size: Int): ValidationBuilder = {
    validationBuilder.expr(s"SIZE($column) >= $size")
  }

  def luhnCheck: ValidationBuilder = {
    validationBuilder.expr(s"LUHN_CHECK($column)")
  }

  def hasType(`type`: String): ValidationBuilder = {
    validationBuilder.expr(s"TYPEOF($column) == '${`type`}'")
  }

  private def colValueToString(value: Any): String = {
    value match {
      case _: String => s"'$value'"
      case _: Date => s"DATE('$value')"
      case _: Timestamp => s"TIMESTAMP('$value')"
      case _ => s"$value"
    }
  }
}

case class GroupByValidationBuilder(
                                     validationBuilder: ValidationBuilder = ValidationBuilder(),
                                     columnValidationBuilder: ColumnValidationBuilder = ColumnValidationBuilder(),
                                     groupByCols: Seq[String] = Seq()
                                   ) {
  def this() = this(ValidationBuilder(), ColumnValidationBuilder(), Seq())

  def sum(column: String): ColumnValidationBuilder = {
    setGroupValidation(column, AGGREGATION_SUM)
  }

  def count(column: String): ColumnValidationBuilder = {
    setGroupValidation(column, AGGREGATION_COUNT)
  }

  def min(column: String): ColumnValidationBuilder = {
    setGroupValidation(column, AGGREGATION_MIN)
  }

  def max(column: String): ColumnValidationBuilder = {
    setGroupValidation(column, AGGREGATION_MAX)
  }

  def avg(column: String): ColumnValidationBuilder = {
    setGroupValidation(column, AGGREGATION_AVG)
  }

  private def setGroupValidation(column: String, aggType: String) = {
    val groupByValidation = GroupByValidation(groupByCols, column, aggType)
    groupByValidation.errorThreshold = validationBuilder.validation.errorThreshold
    groupByValidation.description = validationBuilder.validation.description
    ColumnValidationBuilder(s"$aggType($column)", validationBuilder.modify(_.validation).setTo(groupByValidation))
  }
}

case class AggregationColumnValidationBuilder(groupByValidationBuilder: GroupByValidationBuilder = GroupByValidationBuilder()) {
  def this() = this(GroupByValidationBuilder())


}

case class WaitConditionBuilder(waitCondition: WaitCondition = PauseWaitCondition()) {
  def this() = this(PauseWaitCondition())

  /**
   * Pause for configurable number of seconds, before starting data validations.
   *
   * @param pauseInSeconds Seconds to pause
   * @return WaitConditionBuilder
   */
  def pause(pauseInSeconds: Int): WaitConditionBuilder = this.modify(_.waitCondition).setTo(PauseWaitCondition(pauseInSeconds))

  /**
   * Wait until file exists within path before starting data validations.
   *
   * @param path Path to file
   * @return WaitConditionBuilder
   */
  def file(path: String): WaitConditionBuilder = this.modify(_.waitCondition).setTo(FileExistsWaitCondition(path))

  /**
   * Wait until a specific data condition is met before starting data validations. Specific data condition to be defined
   * as a SQL expression that returns a boolean value. Need to use a data source that is already defined.
   *
   * @param dataSourceName Name of data source that is already defined
   * @param options Additional data source connection options to use to get data
   * @param expr SQL expression that returns a boolean
   * @return WaitConditionBuilder
   */
  def dataExists(dataSourceName: String, options: Map[String, String], expr: String): WaitConditionBuilder =
    this.modify(_.waitCondition).setTo(DataExistsWaitCondition(dataSourceName, options, expr))

  /**
   * Wait until GET request to URL returns back 200 status code, then will start data validations
   *
   * @param url URL for HTTP GET request
   * @return WaitConditionBuilder
   */
  def webhook(url: String): WaitConditionBuilder =
    webhook("tmp_http_data_source", url)

  /**
   * Wait until URL returns back one of the status codes provided before starting data validations.
   *
   * @param url URL for HTTP request
   * @param method HTTP method (i.e. GET, PUT, POST)
   * @param statusCodes HTTP status codes that are treated as successful
   * @return WaitConditionBuilder
   */
  @varargs def webhook(url: String, method: String, statusCodes: Int*): WaitConditionBuilder =
    webhook("tmp_http_data_source", url, method, statusCodes: _*)

  /**
   * Wait until pre-defined HTTP data source with URL, returns back 200 status code from GET request before starting
   * data validations.
   *
   * @param dataSourceName Name of data source already defined
   * @param url URL for HTTP GET request
   * @return WaitConditionBuilder
   */
  def webhook(dataSourceName: String, url: String): WaitConditionBuilder =
    this.modify(_.waitCondition).setTo(WebhookWaitCondition(dataSourceName, url))

  /**
   * Wait until pre-defined HTTP data source with URL, HTTP method and set of successful status codes, return back one
   * of the successful status codes before starting data validations.
   *
   * @param dataSourceName Name of data source already defined
   * @param url URL for HTTP request
   * @param method HTTP method (i.e. GET, PUT, POST)
   * @param statusCode HTTP status codes that are treated as successful
   * @return WaitConditionBuilder
   */
  @varargs def webhook(dataSourceName: String, url: String, method: String, statusCode: Int*): WaitConditionBuilder =
    this.modify(_.waitCondition).setTo(WebhookWaitCondition(dataSourceName, url, method, statusCode.toList))
}