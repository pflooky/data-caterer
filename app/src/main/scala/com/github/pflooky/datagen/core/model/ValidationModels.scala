package com.github.pflooky.datagen.core.model

import com.github.pflooky.datacaterer.api.model.Constants.FORMAT
import com.github.pflooky.datacaterer.api.model.{DataExistsWaitCondition, ExpressionValidation, FileExistsWaitCondition, PauseWaitCondition, Validation, WaitCondition, WebhookWaitCondition}
import com.github.pflooky.datagen.core.exception.InvalidWaitConditionException
import com.github.pflooky.datagen.core.util.HttpUtil.getAuthHeader
import dispatch.Defaults.executor
import dispatch._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

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
                             sampleErrorValues: Option[DataFrame] = None
                           )

object ValidationImplicits {

  implicit class ValidationOps(validation: Validation) {

    def validate(df: DataFrame, dfCount: Long): ValidationResult = {
      validation match {
        case exp: ExpressionValidation => ExpressionValidationOps(exp).validate(df, dfCount)
        case _ => ValidationResult(validation)
      }
    }

    def getIsSuccessAndSampleErrors(notEqualDf: Dataset[Row], dfCount: Long): (Boolean, Option[DataFrame]) = {
      val count = notEqualDf.count()
      val (isSuccess, sampleErrors) = (count, validation.errorThreshold) match {
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

  implicit class ExpressionValidationOps(expressionValidation: ExpressionValidation) extends ValidationOps(expressionValidation) {
    override def validate(df: DataFrame, dfCount: Long): ValidationResult = {
      val notEqualDf = df.where(s"!(${expressionValidation.expr})")
      val (isSuccess, sampleErrors) = ValidationOps(expressionValidation).getIsSuccessAndSampleErrors(notEqualDf, dfCount)
      ValidationResult(expressionValidation, isSuccess, sampleErrors)
    }
  }

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
      val webhookOptions = connectionConfigByName(webhookWaitCondition.dataSourceName)
      val request = dispatch.url(webhookWaitCondition.url)
        .setMethod(webhookWaitCondition.method)
        .setHeaders(getAuthHeader(webhookOptions))
      val responseEither = Http.default(request OK identity).either

      responseEither() match {
        case Left(throwable) =>
          LOGGER.error(s"Failed to execute HTTP wait condition request, url=${webhookWaitCondition.url}", throwable)
          false
        case Right(value) =>
          if (value.getStatusCode == webhookWaitCondition.statusCode) true
          else {
            LOGGER.debug(s"HTTP wait condition status code did not match expected status code, url=${webhookWaitCondition.url}, " +
              s"expected-status-code=${webhookWaitCondition.statusCode}, actual-status-code=${value.getStatusCode}, " +
              s"response-body=${value.getResponseBody}")
            false
          }
      }
    }
  }
}

