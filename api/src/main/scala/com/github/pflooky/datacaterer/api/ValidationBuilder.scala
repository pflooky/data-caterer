package com.github.pflooky.datacaterer.api

import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.github.pflooky.datacaterer.api.connection.{ConnectionTaskBuilder, FileBuilder}
import com.github.pflooky.datacaterer.api.model.Constants.{AGGREGATION_AVG, AGGREGATION_COUNT, AGGREGATION_MAX, AGGREGATION_MIN, AGGREGATION_STDDEV, AGGREGATION_SUM, DEFAULT_VALIDATION_JOIN_TYPE, DEFAULT_VALIDATION_WEBHOOK_HTTP_DATA_SOURCE_NAME, VALIDATION_PREFIX_JOIN_EXPRESSION, VALIDATION_UNIQUE}
import com.github.pflooky.datacaterer.api.model.{DataExistsWaitCondition, DataSourceValidation, ExpressionValidation, FileExistsWaitCondition, GroupByValidation, PauseWaitCondition, UpstreamDataSourceValidation, Validation, ValidationConfiguration, WaitCondition, WebhookWaitCondition}
import com.github.pflooky.datacaterer.api.parser.ValidationBuilderSerializer
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
                               validations: Seq[DataSourceValidation]
                             ): ValidationConfigurationBuilder = {
    val mergedDataSourceValidations = mergeDataSourceValidations(dataSourceName, validations)
    this.modify(_.validationConfiguration.dataSources)(_ ++ Map(dataSourceName -> mergedDataSourceValidations))
  }

  def addDataSourceValidation(
                               dataSourceName: String,
                               validation: DataSourceValidationBuilder
                             ): ValidationConfigurationBuilder = {
    val mergedDataSourceValidations = mergeDataSourceValidations(dataSourceName, validation)
    this.modify(_.validationConfiguration.dataSources)(_ ++ Map(dataSourceName -> mergedDataSourceValidations))
  }

  @varargs def addValidations(
                               dataSourceName: String,
                               options: Map[String, String],
                               validations: ValidationBuilder*
                             ): ValidationConfigurationBuilder =
    addValidations(dataSourceName, options, WaitConditionBuilder(), validations: _*)

  @varargs def addValidations(
                               dataSourceName: String,
                               options: Map[String, String],
                               waitCondition: WaitConditionBuilder,
                               validationBuilders: ValidationBuilder*
                             ): ValidationConfigurationBuilder = {
    val newDsValidation = DataSourceValidationBuilder().options(options).wait(waitCondition).validations(validationBuilders: _*)
    addDataSourceValidation(dataSourceName, newDsValidation)
  }

  private def mergeDataSourceValidations(dataSourceName: String, validation: DataSourceValidationBuilder): List[DataSourceValidation] = {
    validationConfiguration.dataSources.get(dataSourceName)
      .map(listDsValidations => listDsValidations ++ List(validation.dataSourceValidation))
      .getOrElse(List(validation.dataSourceValidation))
  }

  private def mergeDataSourceValidations(dataSourceName: String, validations: Seq[DataSourceValidation]): List[DataSourceValidation] = {
    validationConfiguration.dataSources.get(dataSourceName)
      .map(listDsValidations => listDsValidations ++ validations)
      .getOrElse(validations.toList)
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

@JsonSerialize(using = classOf[ValidationBuilderSerializer])
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
    ColumnValidationBuilder(this, column)
  }

  /**
   * Define columns to group by, so that validation can be run on grouped by dataset
   *
   * @param columns Name of the column to run validation against
   * @return ColumnValidationBuilder
   */
  @varargs def groupBy(columns: String*): GroupByValidationBuilder = {
    GroupByValidationBuilder(this, columns)
  }

  /**
   * Check row count of dataset
   *
   * @return ColumnValidationBuilder to apply validation on row count
   */
  def count(): ColumnValidationBuilder = {
    GroupByValidationBuilder().count()
  }

  /**
   * Check if column(s) values are unique
   *
   * @param columns One or more columns whose values will be checked for uniqueness
   * @return ValidationBuilder
   */
  @varargs def unique(columns: String*): ValidationBuilder = {
    this.modify(_.validation).setTo(GroupByValidation(columns, VALIDATION_UNIQUE, AGGREGATION_COUNT))
      .expr("count == 1")
  }

  /**
   * Define validations based on data in another data source.
   * json(...)
   *   .validations(
   *     validation.upstreamData(csvTask).joinCols("account_id").withValidation(validation.col("upstream_name").isEqualCol("name")),
   *     validation.upstreamData(csvTask).joinCols("account_id").withValidation(validation.expr("upstream_name == name")),
   *     validation.upstreamData(csvTask).joinCols("account_id").withValidation(validation.groupBy("account_id").sum("amount").lessThanCol("balance")),
   *   )
   *
   * @param connectionTaskBuilder
   * @return
   */
  def upstreamData(connectionTaskBuilder: ConnectionTaskBuilder[_]): UpstreamDataSourceValidationBuilder = {
    UpstreamDataSourceValidationBuilder(this, connectionTaskBuilder)
  }
}

case class ColumnValidationBuilder(validationBuilder: ValidationBuilder = ValidationBuilder(), column: String = "") {
  def this() = this(ValidationBuilder(), "")

  def isEqual(value: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column == ${colValueToString(value)}")
  }

  def isEqualCol(value: String): ValidationBuilder = {
    validationBuilder.expr(s"$column == $value")
  }

  def isNotEqual(value: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column != ${colValueToString(value)}")
  }

  def isNotEqualCol(value: String): ValidationBuilder = {
    validationBuilder.expr(s"$column != $value")
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

  def lessThanCol(value: String): ValidationBuilder = {
    validationBuilder.expr(s"$column < $value")
  }

  def lessThanOrEqual(value: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column <= ${colValueToString(value)}")
  }

  def lessThanOrEqualCol(value: String): ValidationBuilder = {
    validationBuilder.expr(s"$column <= $value")
  }

  def greaterThan(value: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column > ${colValueToString(value)}")
  }

  def greaterThanCol(value: String): ValidationBuilder = {
    validationBuilder.expr(s"$column > $value")
  }

  def greaterThanOrEqual(value: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column >= ${colValueToString(value)}")
  }

  def greaterThanOrEqualCol(value: String): ValidationBuilder = {
    validationBuilder.expr(s"$column >= $value")
  }

  def between(minValue: Any, maxValue: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column BETWEEN ${colValueToString(minValue)} AND ${colValueToString(maxValue)}")
  }

  def betweenCol(minValue: String, maxValue: String): ValidationBuilder = {
    validationBuilder.expr(s"$column BETWEEN $minValue AND $maxValue")
  }

  def notBetween(minValue: Any, maxValue: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column NOT BETWEEN ${colValueToString(minValue)} AND ${colValueToString(maxValue)}")
  }

  def notBetweenCol(minValue: String, maxValue: String): ValidationBuilder = {
    validationBuilder.expr(s"$column NOT BETWEEN $minValue AND $maxValue")
  }

  @varargs def in(values: Any*): ValidationBuilder = {
    validationBuilder.expr(s"$column IN (${values.map(colValueToString).mkString(",")})")
  }

  @varargs def notIn(values: Any*): ValidationBuilder = {
    validationBuilder.expr(s"NOT $column IN (${values.map(colValueToString).mkString(",")})")
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

  def expr(expr: String): ValidationBuilder = {
    validationBuilder.expr(expr)
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
                                     groupByCols: Seq[String] = Seq()
                                   ) {
  def this() = this(ValidationBuilder(), Seq())

  def sum(column: String): ColumnValidationBuilder = {
    setGroupValidation(column, AGGREGATION_SUM)
  }

  def count(column: String): ColumnValidationBuilder = {
    setGroupValidation(column, AGGREGATION_COUNT)
  }

  def count(): ColumnValidationBuilder = {
    setGroupValidation("", AGGREGATION_COUNT)
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

  def stddev(column: String): ColumnValidationBuilder = {
    setGroupValidation(column, AGGREGATION_STDDEV)
  }

  private def setGroupValidation(column: String, aggType: String): ColumnValidationBuilder = {
    val groupByValidation = GroupByValidation(groupByCols, column, aggType)
    groupByValidation.errorThreshold = validationBuilder.validation.errorThreshold
    groupByValidation.description = validationBuilder.validation.description
    val colName = if (column.isEmpty) aggType else s"$aggType($column)"
    ColumnValidationBuilder(validationBuilder.modify(_.validation).setTo(groupByValidation), colName)
  }
}

case class UpstreamDataSourceValidationBuilder(
                                                validationBuilder: ValidationBuilder = ValidationBuilder(),
                                                connectionTaskBuilder: ConnectionTaskBuilder[_] = FileBuilder(),
                                                readOptions: Map[String, String] = Map(),
                                                joinColumns: List[String] = List(),
                                                joinType: String = DEFAULT_VALIDATION_JOIN_TYPE
                                              ) {
  def this() = this(ValidationBuilder(), FileBuilder(), Map(), List(), DEFAULT_VALIDATION_JOIN_TYPE)

  def readOptions(readOptions: Map[String, String]): UpstreamDataSourceValidationBuilder = {
    this.modify(_.readOptions).setTo(readOptions)
  }

  @varargs def joinColumns(joinCols: String*): UpstreamDataSourceValidationBuilder = {
    this.modify(_.joinColumns).setTo(joinCols.toList)
  }

  def joinExpr(expr: String): UpstreamDataSourceValidationBuilder = {
    this.modify(_.joinColumns).setTo(List(s"$VALIDATION_PREFIX_JOIN_EXPRESSION$expr"))
  }

  def joinType(joinType: String): UpstreamDataSourceValidationBuilder = {
    this.modify(_.joinType).setTo(joinType)
  }

  def withValidation(validationBuilder: ValidationBuilder): ValidationBuilder = {
    validationBuilder.modify(_.validation).setTo(UpstreamDataSourceValidation(validationBuilder, connectionTaskBuilder, readOptions, joinColumns, joinType))
  }
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
   * @param options        Additional data source connection options to use to get data
   * @param expr           SQL expression that returns a boolean
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
    webhook(DEFAULT_VALIDATION_WEBHOOK_HTTP_DATA_SOURCE_NAME, url)

  /**
   * Wait until URL returns back one of the status codes provided before starting data validations.
   *
   * @param url         URL for HTTP request
   * @param method      HTTP method (i.e. GET, PUT, POST)
   * @param statusCodes HTTP status codes that are treated as successful
   * @return WaitConditionBuilder
   */
  @varargs def webhook(url: String, method: String, statusCodes: Int*): WaitConditionBuilder =
    webhook(DEFAULT_VALIDATION_WEBHOOK_HTTP_DATA_SOURCE_NAME, url, method, statusCodes: _*)

  /**
   * Wait until pre-defined HTTP data source with URL, returns back 200 status code from GET request before starting
   * data validations.
   *
   * @param dataSourceName Name of data source already defined
   * @param url            URL for HTTP GET request
   * @return WaitConditionBuilder
   */
  def webhook(dataSourceName: String, url: String): WaitConditionBuilder =
    this.modify(_.waitCondition).setTo(WebhookWaitCondition(dataSourceName, url))

  /**
   * Wait until pre-defined HTTP data source with URL, HTTP method and set of successful status codes, return back one
   * of the successful status codes before starting data validations.
   *
   * @param dataSourceName Name of data source already defined
   * @param url            URL for HTTP request
   * @param method         HTTP method (i.e. GET, PUT, POST)
   * @param statusCode     HTTP status codes that are treated as successful
   * @return WaitConditionBuilder
   */
  @varargs def webhook(dataSourceName: String, url: String, method: String, statusCode: Int*): WaitConditionBuilder =
    this.modify(_.waitCondition).setTo(WebhookWaitCondition(dataSourceName, url, method, statusCode.toList))
}