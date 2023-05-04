package com.github.pflooky.datagen.core.generator.plan.datasource.database

import org.apache.spark.sql.{Dataset, SparkSession}

case class MysqlMetadata(name: String, connectionConfig: Map[String, String]) extends JdbcMetadata {
  override val baseFilterSchema: List[String] = List("sys", "information_schema", "mysql", "performance_schema")

  override def getAdditionalColumnMetadata(implicit sparkSession: SparkSession): Dataset[ColumnMetadata] = ???

  override def getForeignKeys(implicit sparkSession: SparkSession): Dataset[ForeignKeyRelationship] = ???
}
