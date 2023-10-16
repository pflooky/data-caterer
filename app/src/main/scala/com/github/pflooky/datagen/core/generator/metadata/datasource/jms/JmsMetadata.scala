package com.github.pflooky.datagen.core.generator.metadata.datasource.jms

import com.github.pflooky.datacaterer.api.model.Constants.{PATH, SCHEMA_LOCATION}
import com.github.pflooky.datagen.core.generator.metadata.datasource.{DataSourceMetadata, SubDataSourceMetadata}
import com.github.pflooky.datagen.core.generator.metadata.datasource.database.ColumnMetadata
import org.apache.spark.sql.{Dataset, SparkSession}

case class JmsMetadata(name: String, format: String, connectionConfig: Map[String, String]) extends DataSourceMetadata {

  override val hasSourceData: Boolean = false
  override def toStepName(options: Map[String, String]): String = {
    options(PATH)
  }

  override def getSubDataSourcesMetadata(implicit sparkSession: SparkSession): Array[SubDataSourceMetadata] = {
    //TODO schema location can be either API endpoint or file, for now only allow file
    val schemaLocation = connectionConfig(SCHEMA_LOCATION)
    //could be JSON, Avro, Protobuf or XML schema definition
    //only avro and protobuf define schema with fields and types
    //find all avro/profobuf files under schema location directory

    //or example is defined and schema is learned from the example
    Array()
  }
}
