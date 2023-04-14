package com.github.pflooky.datagen.core.generator.plan.datasource

import com.github.pflooky.datagen.core.model.Constants.{CASSANDRA_KEYSPACE, CASSANDRA_TABLE, JDBC_TABLE}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DatabaseMetadata(sparkSession: SparkSession) {

  def getCassandraTableMetadata(connectionConfig: Map[String, String], filterSchema: List[String], filterTable: List[String]): DataFrame = {
    val options = connectionConfig ++ Map(
      CASSANDRA_KEYSPACE -> "system_schema",
      CASSANDRA_TABLE -> "tables"
    )
    val selectExpr = List("keyspace_name AS schema", "table_name AS table")
    getMetadata(options, selectExpr)
  }

  def getPostgresTableMetadata(connectionConfig: Map[String, String], filterSchema: List[String] = List("pg_catalogs", "information_schema"), filterTable: List[String]): DataFrame = {
    val options = connectionConfig ++ Map(JDBC_TABLE -> "information_schema.tables")
    val selectExpr = List("table_schema AS schema", "table_name AS table")
    getMetadata(options, selectExpr)
      .filter("schema != 'pg_catalogs' AND schema != 'information_schema'")
  }

  private def getMetadata(options: Map[String, String], selectExpr: List[String]): DataFrame = {
    sparkSession.read.options(options).load().selectExpr(selectExpr: _*)
  }
}
