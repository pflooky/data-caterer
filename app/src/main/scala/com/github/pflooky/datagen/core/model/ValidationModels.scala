package com.github.pflooky.datagen.core.model

import com.github.pflooky.datacaterer.api.model.Constants.{AGGREGATION_COUNT, FORMAT, VALIDATION_UNIQUE}
import com.github.pflooky.datacaterer.api.model.{DataExistsWaitCondition, ExpressionValidation, FileExistsWaitCondition, GroupByValidation, PauseWaitCondition, Validation, ValidationConfiguration, WaitCondition, WebhookWaitCondition}
import com.github.pflooky.datagen.core.exception.InvalidWaitConditionException
import com.github.pflooky.datagen.core.util.HttpUtil.getAuthHeader
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.asynchttpclient.Dsl.asyncHttpClient

import scala.util.{Failure, Success, Try}

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

object ValidationConfigurationHelper {
  /**
   * A few different scenarios:
   * - user defined validations, no generated validations
   * - user defined validations, generated validations
   * - user defined validation for 1 data source, generated validations for 2 data sources
   * -
   *
   * @param userValidationConf
   * @param generatedValidationConf
   * @return
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

object ValidationImplicits {

  implicit class ValidationOps(validation: Validation) {

    def validate(df: DataFrame, dfCount: Long): ValidationResult = {
      validation match {
        case exp: ExpressionValidation => ExpressionValidationOps(exp).validate(df, dfCount)
        case grp: GroupByValidation => GroupByValidationOps(grp).validate(df, dfCount)
        case _ => ValidationResult(validation)
      }
    }

    def getIsSuccessAndSampleErrors(notEqualDf: Dataset[Row], dfCount: Long): (Boolean, Option[DataFrame], Long) = {
      val numErrors = notEqualDf.count()
      val (isSuccess, sampleErrors) = (numErrors, validation.errorThreshold) match {
        case (c, Some(threshold)) if c > 0 =>
          if ((threshold >= 1 && c > threshold) || (threshold < 1 && c.toDouble / dfCount > threshold)) {
            (false, Some(notEqualDf))
          } else (true, None)
        case (c, None) if c > 0 => (false, Some(notEqualDf))
        case _ => (true, None)
      }
      (isSuccess, sampleErrors, numErrors)
    }
  }

  implicit class ExpressionValidationOps(expressionValidation: ExpressionValidation) extends ValidationOps(expressionValidation) {
    override def validate(df: DataFrame, dfCount: Long): ValidationResult = {
      val notEqualDf = df.where(s"!(${expressionValidation.expr})")
      val (isSuccess, sampleErrors, numErrors) = ValidationOps(expressionValidation).getIsSuccessAndSampleErrors(notEqualDf, dfCount)
      ValidationResult(expressionValidation, isSuccess, numErrors, dfCount, sampleErrors)
    }
  }

  implicit class GroupByValidationOps(groupByValidation: GroupByValidation) extends ValidationOps(groupByValidation) {
    override def validate(df: DataFrame, dfCount: Long): ValidationResult = {
      val groupByDf = df.groupBy(groupByValidation.groupByCols.map(col): _*)
      val aggregateDf = if (groupByValidation.aggCol == VALIDATION_UNIQUE && groupByValidation.aggType == AGGREGATION_COUNT) {
        groupByDf.count()
      } else {
        groupByDf.agg(Map(
          groupByValidation.aggCol -> groupByValidation.aggType
        ))
      }
      val notEqualDf = aggregateDf.where(s"!(${groupByValidation.expr})")
      val (isSuccess, sampleErrors, numErrors) = ValidationOps(groupByValidation).getIsSuccessAndSampleErrors(notEqualDf, dfCount)
      ValidationResult(groupByValidation, isSuccess, numErrors, dfCount, sampleErrors)
    }
  }
  //second argument can have both sides expressed as SQL expressions
  //validation.exists(csvTask, "account_number" -> "REPLACE(account_id, 'ACC', '')", <optional number per account_number exists, by default one to one match>)
  //additional verifications afterwards?
  //
  //  implicit class ScanValidationOps(scanValidation: ScanValidation) extends ValidationOps(scanValidation) {
  //    override def validate(df: DataFrame, dfCount: Long): ValidationResult = {
  //      scanValidation.check match {
  //        case VALIDATION_UNIQUE =>
  //          val duplicates = df.groupBy(scanValidation.columns.map(col): _*).count().filter("count > 1")
  //
  //      }
  //
  //    }
  //  }

  implicit class WaitConditionOps(waitCondition: WaitCondition = PauseWaitCondition()) {
    def checkCondition(implicit sparkSession: SparkSession): Boolean = true

    def checkCondition(connectionConfigByName: Map[String, Map[String, String]])(implicit sparkSession: SparkSession): Boolean = true

    def waitForCondition(connectionConfigByName: Map[String, Map[String, String]])(implicit sparkSession: SparkSession): Unit = {
      if (waitCondition.isRetryable) {
        var retries = 0
        while (retries < waitCondition.maxRetries) {
          val isDataAvailable = waitCondition match {
            case DataExistsWaitCondition(_, _, _) | WebhookWaitCondition(_, _, _, _) => this.checkCondition(connectionConfigByName)
            case FileExistsWaitCondition(_) => this.checkCondition
            case x => throw new InvalidWaitConditionException(x.getClass.getName)
          }
          if (!isDataAvailable) {
            Thread.sleep(waitCondition.waitBeforeRetrySeconds * 1000)
            retries += 1
          } else {
            return
          }
        }
      } else {
        this.checkCondition
      }
    }
  }

  implicit class PauseWaitConditionOps(pauseWaitCondition: PauseWaitCondition) extends WaitConditionOps(pauseWaitCondition) {
    override def checkCondition(implicit sparkSession: SparkSession): Boolean = {
      Thread.sleep(pauseWaitCondition.pauseInSeconds * 1000)
      true
    }
  }

  implicit class FileExistsWaitConditionOps(fileExistsWaitCondition: FileExistsWaitCondition) extends WaitConditionOps(fileExistsWaitCondition) {
    override def checkCondition(implicit sparkSession: SparkSession): Boolean = {
      val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      fs.exists(new Path(fileExistsWaitCondition.path))
    }
  }

  implicit class DataExistsWaitConditionOps(dataExistsWaitCondition: DataExistsWaitCondition) extends WaitConditionOps(dataExistsWaitCondition) {
    override def checkCondition(connectionConfigByName: Map[String, Map[String, String]])(implicit sparkSession: SparkSession): Boolean = {
      val connectionOptions = connectionConfigByName(dataExistsWaitCondition.dataSourceName)
      val loadData = sparkSession.read
        .format(connectionOptions(FORMAT))
        .options(connectionOptions ++ dataExistsWaitCondition.options)
        .load()
        .where(dataExistsWaitCondition.expr)
      !loadData.isEmpty
    }
  }

  implicit class WebhookWaitConditionOps(webhookWaitCondition: WebhookWaitCondition) extends WaitConditionOps(webhookWaitCondition) {
    private val LOGGER = Logger.getLogger(getClass.getName)

    override def checkCondition(connectionConfigByName: Map[String, Map[String, String]])(implicit sparkSession: SparkSession): Boolean = {
      val webhookOptions = connectionConfigByName.getOrElse(webhookWaitCondition.dataSourceName, Map())
      val request = asyncHttpClient().prepare(webhookWaitCondition.method, webhookWaitCondition.url)
      val authHeader = getAuthHeader(webhookOptions)
      val requestWithAuth = if (authHeader.nonEmpty) request.setHeader(authHeader.head._1, authHeader.head._2) else request

      val tryResponse = Try(requestWithAuth.execute().get())

      tryResponse match {
        case Failure(exception) =>
          LOGGER.error(s"Failed to execute HTTP wait condition request, url=${webhookWaitCondition.url}", exception)
          false
        case Success(value) =>
          if (webhookWaitCondition.statusCodes.contains(value.getStatusCode)) {
            true
          } else {
            LOGGER.debug(s"HTTP wait condition status code did not match expected status code, url=${webhookWaitCondition.url}, " +
              s"expected-status-code=${webhookWaitCondition.statusCodes}, actual-status-code=${value.getStatusCode}, " +
              s"response-body=${value.getResponseBody}")
            false
          }
      }
    }
  }
}

