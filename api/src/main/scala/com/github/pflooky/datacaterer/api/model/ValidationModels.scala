package com.github.pflooky.datacaterer.api.model

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonTypeInfo}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonTypeIdResolver}
import com.github.pflooky.datacaterer.api.parser.ValidationIdResolver


@JsonTypeInfo(use = Id.CUSTOM, defaultImpl = classOf[ExpressionValidation])
@JsonTypeIdResolver(classOf[ValidationIdResolver])
@JsonIgnoreProperties(ignoreUnknown = true)
trait Validation {
  var description: Option[String] = None
  @JsonDeserialize(contentAs = classOf[java.lang.Double]) var errorThreshold: Option[Double] = None
}

case class ExpressionValidation(
                                 expr: String = "true"
                               ) extends Validation

case class ValidationConfiguration(
                                    name: String = "default_validation",
                                    description: String = "Validation of data sources after generating data",
                                    dataSources: Map[String, DataSourceValidation] = Map()
                                  )

case class DataSourceValidation(
                                 options: Map[String, String] = Map(),
                                 waitCondition: WaitCondition = PauseWaitCondition(),
                                 validations: List[Validation] = List()
                               )

trait WaitCondition {
  val isRetryable: Boolean
  val maxRetries: Int = 10
  val waitBeforeRetrySeconds: Int = 2
}

case class PauseWaitCondition(
                               pauseInSeconds: Int = 0,
                               override val isRetryable: Boolean = false
                             ) extends WaitCondition

case class FileExistsWaitCondition(
                                    path: String,
                                    override val isRetryable: Boolean = true
                                  ) extends WaitCondition

case class DataExistsWaitCondition(
                                    dataSourceName: String,
                                    options: Map[String, String],
                                    expr: String,
                                    override val isRetryable: Boolean = true
                                  ) extends WaitCondition

case class WebhookWaitCondition(
                                 dataSourceName: String,
                                 url: String,
                                 method: String = "GET",
                                 statusCode: Int = 200,
                                 override val isRetryable: Boolean = true
                               ) extends WaitCondition