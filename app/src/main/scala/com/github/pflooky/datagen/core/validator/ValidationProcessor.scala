package com.github.pflooky.datagen.core.validator

import com.github.pflooky.datacaterer.api.model.Constants.{FORMAT, HTTP, JMS}
import com.github.pflooky.datacaterer.api.model.{DataSourceValidation, ExpressionValidation, GroupByValidation, ValidationConfiguration}
import com.github.pflooky.datagen.core.model.ValidationImplicits.{ValidationOps, WaitConditionOps}
import com.github.pflooky.datagen.core.model.{DataSourceValidationResult, ValidationConfigResult}
import com.github.pflooky.datagen.core.parser.ValidationParser
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
Given a list of validations, check and report on the success and failure of each
Flag to enable
Validations can occur on any data source defined in application config
Validations will only occur on datasets not on the response from the data source (i.e. no HTTP status code validations)
Defined at plan level what validations are run post data generation
Validations lie within separate files
Validations have a wait condition. Wait for: webhook, pause, file exists, data exists
Different types of validations:
- simple column validations (amount < 100)
- aggregates (sum of amount per account is > 500)
- ordering (transactions are ordered by date)
- relationship (at least one account entry in history table per account in accounts table)
- data profile (how close the generated data profile is compared to the expected data profile)
 */
class ValidationProcessor(
                           connectionConfigsByName: Map[String, Map[String, String]],
                           optValidationConfigs: Option[List[ValidationConfiguration]],
                           validationFolderPath: String
                         )(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def executeValidations: List[ValidationConfigResult] = {
    LOGGER.info("Executing data validations")
    val validationResults = getValidations.map(vc => {
      val dataSourceValidationResults = vc.dataSources.flatMap(dataSource => {
        val dataSourceName = dataSource._1
        val dataSourceValidations = dataSource._2
        val numValidations = dataSourceValidations.flatMap(_.validations).size

        LOGGER.info(s"Executing data validations for data source, name=${vc.name}," +
          s"data-source-name=$dataSourceName, num-validations=$numValidations")
        dataSourceValidations.map(dataSourceValidation => executeDataValidations(vc, dataSourceName, dataSourceValidation))
      }).toList
      ValidationConfigResult(vc.name, vc.description, dataSourceValidationResults)
    }).toList

    logValidationErrors(validationResults)
    validationResults
  }

  private def executeDataValidations(
                                      vc: ValidationConfiguration,
                                      dataSourceName: String,
                                      dataSourceValidation: DataSourceValidation
                                    ): DataSourceValidationResult = {
    LOGGER.debug(s"Waiting for validation condition to be successful before running validations, name=${vc.name}," +
      s"data-source-name=$dataSourceName, details=${dataSourceValidation.options}, num-validations=${dataSourceValidation.validations.size}")
    dataSourceValidation.waitCondition.waitForCondition(connectionConfigsByName)

    val df = getDataFrame(dataSourceName, dataSourceValidation.options)
    val count = df.count()
    if (count == 0) {
      LOGGER.info("No data validations found")
      DataSourceValidationResult(dataSourceName, dataSourceValidation.options, List())
    } else {
      val results = dataSourceValidation.validations.map(validBuilder => validBuilder.validation.validate(df, count))
      df.unpersist()
      LOGGER.debug(s"Finished data validations, name=${vc.name}," +
        s"data-source-name=$dataSourceName, details=${dataSourceValidation.options}, num-validations=${dataSourceValidation.validations.size}")
      DataSourceValidationResult(dataSourceName, dataSourceValidation.options, results)
    }
  }

  private def getValidations: Array[ValidationConfiguration] = {
    optValidationConfigs.map(_.toArray).getOrElse(ValidationParser.parseValidation(validationFolderPath))
  }

  private def getDataFrame(dataSourceName: String, options: Map[String, String]): DataFrame = {
    val connectionConfig = connectionConfigsByName(dataSourceName)
    val format = connectionConfig(FORMAT)
    if (format == HTTP || format == JMS) {
      LOGGER.warn("No support for HTTP or JMS data validations, will skip validations")
      sparkSession.emptyDataFrame
    } else {
      val df = sparkSession.read
        .format(format)
        .options(connectionConfig ++ options)
        .load()
      if (!df.storageLevel.useMemory) df.cache()
      df
    }
  }

  private def logValidationErrors(validationResults: List[ValidationConfigResult]): Unit = {
    validationResults.foreach(vcr => vcr.dataSourceValidationResults.map(dsr => {
      val failedValidations = dsr.validationResults.filter(r => !r.isSuccess)

      if (failedValidations.isEmpty) {
        LOGGER.info(s"Data validations successful for validation, name=${vcr.name}, description=${vcr.description}, data-source-name=${dsr.dataSourceName}, " +
          s"data-source-options=${dsr.options}, is-success=true")
      } else {
        failedValidations.foreach(validationRes => {
          val (validationType, validationCheck) = validationRes.validation match {
            case ExpressionValidation(expr) => ("expression", expr)
            case GroupByValidation(_, _, _, expr) => ("groupByAggregate", expr)
            case _ => ("Unknown", "")
          }
          val sampleErrors = validationRes.sampleErrorValues.get.take(5).map(_.json).mkString(",")
          LOGGER.error(s"Failed validation: validation-name=${vcr.name}, description=${vcr.description}, data-source-name=${dsr.dataSourceName}, " +
            s"data-source-options=${dsr.options}, is-success=${validationRes.isSuccess}, validation-type=$validationType, check=$validationCheck, sample-errors=$sampleErrors")
        })
      }
    }))
  }
}
