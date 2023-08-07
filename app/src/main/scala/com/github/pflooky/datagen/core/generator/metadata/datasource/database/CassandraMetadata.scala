package com.github.pflooky.datagen.core.generator.metadata.datasource.database

import com.github.pflooky.datagen.core.model.Constants.{CASSANDRA, CASSANDRA_KEYSPACE, CASSANDRA_TABLE}
import org.apache.spark.sql.{Dataset, SparkSession}

case class CassandraMetadata(name: String, connectionConfig: Map[String, String]) extends DatabaseMetadata {
  override val format: String = CASSANDRA

  override val metadataTable: Map[String, String] = Map(CASSANDRA_KEYSPACE -> "system_schema", CASSANDRA_TABLE -> "tables")

  override val selectExpr: List[String] = List("keyspace_name AS schema", "table_name AS table")

  override def getTableDataOptions(schema: String, table: String): Map[String, String] = {
    Map(CASSANDRA_KEYSPACE -> schema, CASSANDRA_TABLE -> table)
  }

  override def getAdditionalColumnMetadata(implicit sparkSession: SparkSession): Dataset[ColumnMetadata] = ???

  override def getForeignKeys(implicit sparkSession: SparkSession): Dataset[ForeignKeyRelationship] = ???

  override def toStepName(options: Map[String, String]): String = {
    s"${name}_${options(CASSANDRA_KEYSPACE)}_${options(CASSANDRA_TABLE)}"
  }
}
