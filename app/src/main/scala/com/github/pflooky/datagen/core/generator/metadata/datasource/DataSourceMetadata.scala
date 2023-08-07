package com.github.pflooky.datagen.core.generator.metadata.datasource

import com.github.pflooky.datagen.core.generator.metadata.datasource.database.{ColumnMetadata, ForeignKeyRelationship}
import org.apache.spark.sql.{Dataset, SparkSession}

trait DataSourceMetadata {
  val name: String
  val format: String
  val connectionConfig: Map[String, String]

  def getAdditionalColumnMetadata(implicit sparkSession: SparkSession): Dataset[ColumnMetadata]

  def getForeignKeys(implicit sparkSession: SparkSession): Dataset[ForeignKeyRelationship]

  def toStepName(options: Map[String, String]): String
}
