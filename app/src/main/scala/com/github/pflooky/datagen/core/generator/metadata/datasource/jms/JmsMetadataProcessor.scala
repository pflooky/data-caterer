package com.github.pflooky.datagen.core.generator.metadata.datasource.jms

import com.github.pflooky.datacaterer.api.model.Constants.SCHEMA_LOCATION
import com.github.pflooky.datagen.core.generator.metadata.datasource.{DataSourceMetadata, MetadataProcessor}
import org.apache.spark.sql.SparkSession

class JmsMetadataProcessor(override val dataSourceMetadata: DataSourceMetadata)(implicit sparkSession: SparkSession) extends MetadataProcessor {
  override def getSubDataSourcesMetadata: Array[Map[String, String]] = {
    //TODO schema location can be either API endpoint or file, for now only allow file
    val schemaLocation = dataSourceMetadata.connectionConfig(SCHEMA_LOCATION)
    //could be JSON, Avro, Protobuf or XML schema definition
    //only avro and protobuf define schema with fields and types
    //find all avro/profobuf files under schema location directory

    //or example is defined and schema is learned from the example
    Array()
  }
}
