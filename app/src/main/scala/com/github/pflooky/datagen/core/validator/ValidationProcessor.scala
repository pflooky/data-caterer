package com.github.pflooky.datagen.core.validator

import com.github.pflooky.datagen.core.model.Constants.FORMAT
import com.github.pflooky.datagen.core.model.{DataSourceValidationResult, ExpressionValidation, ValidationConfigResult}
import com.github.pflooky.datagen.core.parser.ValidationParser
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/*
Given a list of validations, check and report on the success and failure of each
Flag to enable
Validations can occur on any data source defined in application config
Validations will only occur on datasets not on the response from the data source (i.e. no HTTP status code validations)
Defined at plan level what validations are run post data generation
Validations lie within separate files
Different types of validations:
- simple column validations (amount < 100)
- aggregates (sum of amount per account is > 500)
- ordering (transactions are ordered by date)
- relationship (at least one account entry in history table per account in accounts table)
- data profile (how close the generated data profile is compared to the expected data profile)
 */
class ValidationProcessor(connectionConfigsByName: Map[String, Map[String, String]], validationFolderPath: String)
                         (implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val validationConfig = ValidationParser.parseValidation(validationFolderPath)

  def executeValidations: List[ValidationConfigResult] = {
    val validationResults = validationConfig.map(vc => {
      val dataSourceValidationResults = vc.dataSources.map(ds => {
        val dataSourceName = ds._1
        val connectionConfig = connectionConfigsByName(dataSourceName)
        val format = connectionConfig(FORMAT)
        val df = sparkSession.read.format(format)
          .options(connectionConfig ++ ds._2.options)
          .load()
        if (!df.storageLevel.useMemory) df.cache()
        val count = df.count()
        val results = ds._2.validations.map(validation => validation.validate(df, count))
        df.unpersist()
        DataSourceValidationResult(dataSourceName, ds._2.options, results)
      }).toList
      ValidationConfigResult(vc.name, vc.description, dataSourceValidationResults)
    }).toList

    logValidationErrors(validationResults)
    validationResults
  }

  private def logValidationErrors(validationResults: List[ValidationConfigResult]): Unit = {
    validationResults.foreach(vcr => vcr.dataSourceValidationResults.map(dsr => {
      val failedValidations = dsr.validationResults.filter(r => !r.isSuccess)
      failedValidations.foreach(validationRes => {
        val (validationType, validationCheck) = validationRes.validation match {
          case ExpressionValidation(expr) => ("expression", expr)
          case _ => ("Unknown", "")
        }
        val sampleErrors = validationRes.sampleErrorValues.get.take(5).map(_.json).mkString(",")
        LOGGER.error(s"Failed validation: validation-name=${vcr.name}, description=${vcr.description}, data-source-name=${dsr.dataSourceName}, " +
          s"data-source-options=${dsr.options}, is-success=${validationRes.isSuccess}, validation-type=$validationType, check=$validationCheck, sample-errors=$sampleErrors")
      })
    }))
  }
}
