package com.github.pflooky.datagen.core.generator.metadata.datasource.http

import com.github.pflooky.datacaterer.api.model.Constants.{PATH, SCHEMA_LOCATION}
import com.github.pflooky.datagen.core.generator.metadata.datasource.DataSourceMetadata
import io.swagger.v3.oas.models.media.Schema
import io.swagger.v3.parser.OpenAPIV3Parser
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.mapAsScalaMapConverter

case class HttpMetadata(name: String, format: String, connectionConfig: Map[String, String]) extends DataSourceMetadata {

  private val LOGGER = Logger.getLogger(getClass.getName)

  override val hasSourceData: Boolean = false

  override def toStepName(options: Map[String, String]): String = {
    options(PATH)
  }

  override def getSubDataSourcesMetadata(implicit sparkSession: SparkSession): Array[Map[String, String]] = {
    connectionConfig.get(SCHEMA_LOCATION) match {
      case Some(location) =>
        //validate the file is openapi endpoint/doc
        //return back all endpoints along with any metadata from the doc
        val openApiSpec = new OpenAPIV3Parser().read(location)
        openApiSpec.getPaths.asScala.map(path => {
          path._2.readOperationsMap()
            .asScala
            .map(pathOperation => {
              val requestContent = pathOperation._2.getRequestBody.getContent.asScala.head
              val requestContentType = requestContent._1
              val schema = requestContent._2.getSchema
              //              val headers = pathOperation._2.getParameters.asScala
              //                .filter(p => p.getIn == "header")
              //                .map(p => (s"$HTTP_HEADER_PREFIX.${p.getName}", ""))
              //
              //              Map(
              //                HTTP_METHOD -> pathOperation._1.name(),
              //                HTTP_CONTENT_TYPE -> requestContent._1,
              //              ) ++
              //                headers
              Map()
            })
        })
        Array()
      case None =>
        LOGGER.warn(s"No $SCHEMA_LOCATION defined, unable to extract out metadata for http data source. Please define $SCHEMA_LOCATION " +
          s"as either an endpoint or file location to the OpenAPI specification for your http endpoints, name=$name")
        Array()
    }
  }

  private def schemaToMap(schema: Schema[_]): Map[String, String] = {
    if (schema.getType.toLowerCase == "object") {
      //then there is an inner struct defined and need to loop

    } else {
      //simple case where it is specific field

    }
    Map()
  }
}
