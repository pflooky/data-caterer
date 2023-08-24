package com.github.pflooky.datagen.core.model

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonTypeInfo}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonTypeIdResolver}
import com.github.pflooky.datagen.core.parser.ValidationIdResolver
import org.apache.spark.sql.{DataFrame, Dataset, Row}


@JsonTypeInfo(use = Id.CUSTOM, defaultImpl = classOf[ExpressionValidation])
@JsonTypeIdResolver(classOf[ValidationIdResolver])
@JsonIgnoreProperties(ignoreUnknown = true)
trait Validation {
  val description: Option[String] = None
  @JsonDeserialize(contentAs = classOf[java.lang.Double]) val errorThreshold: Option[Double] = None

  def validate(df: DataFrame, dfCount: Long): ValidationResult

  def getIsSuccessAndSampleErrors(notEqualDf: Dataset[Row], dfCount: Long): (Boolean, Option[DataFrame]) = {
    val count = notEqualDf.count()
    val (isSuccess, sampleErrors) = (count, errorThreshold) match {
      case (c, Some(threshold)) if c > 0 =>
        if ((threshold >= 1 && c > threshold) || (threshold < 1 && c.toDouble / dfCount > threshold)) {
          (false, Some(notEqualDf))
        } else (true, None)
      case (c, None) if c > 0 => (false, Some(notEqualDf))
      case _ => (true, None)
    }
    (isSuccess, sampleErrors)
  }
}

case class ExpressionValidation(expr: String) extends Validation {

  override def validate(df: DataFrame, dfCount: Long): ValidationResult = {
    val notEqualDf = df.where(s"!($expr)")
    val (isSuccess, sampleErrors) = getIsSuccessAndSampleErrors(notEqualDf, dfCount)
    ValidationResult(this, isSuccess, sampleErrors)
  }
}

case class ValidationConfigResult(name: String, description: String, dataSourceValidationResults: List[DataSourceValidationResult])

case class DataSourceValidationResult(dataSourceName: String, options: Map[String, String], validationResults: List[ValidationResult])

case class ValidationResult(validation: Validation, isSuccess: Boolean = true, sampleErrorValues: Option[DataFrame] = None)

case class ValidationConfiguration(name: String, description: String = "Validation of data sources after generating data", dataSources: Map[String, DataSourceValidation] = Map())

case class DataSourceValidation(options: Map[String, String] = Map(), validations: List[Validation] = List())
