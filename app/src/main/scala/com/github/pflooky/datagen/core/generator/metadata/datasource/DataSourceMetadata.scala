package com.github.pflooky.datagen.core.generator.metadata.datasource

import com.github.pflooky.datagen.core.generator.metadata.datasource.database.{ColumnMetadata, ForeignKeyRelationship}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

trait DataSourceMetadata {
  implicit val columnMetadataEncoder: Encoder[ColumnMetadata] = Encoders.kryo[ColumnMetadata]
  implicit val foreignKeyRelationshipEncoder: Encoder[ForeignKeyRelationship] = Encoders.kryo[ForeignKeyRelationship]

  val name: String
  val format: String
  val connectionConfig: Map[String, String]
  val hasSourceData: Boolean

  def getAdditionalColumnMetadata(implicit sparkSession: SparkSession): Dataset[ColumnMetadata] = {
    sparkSession.emptyDataset[ColumnMetadata]
  }

  def getForeignKeys(implicit sparkSession: SparkSession): Dataset[ForeignKeyRelationship] = {
    sparkSession.emptyDataset[ForeignKeyRelationship]
  }

  def close(): Unit = {}

  def getSubDataSourcesMetadata(implicit sparkSession: SparkSession): Array[SubDataSourceMetadata]

  def toStepName(options: Map[String, String]): String
}

case class SubDataSourceMetadata(readOptions: Map[String, String] = Map(), optColumnMetadata: Option[Dataset[ColumnMetadata]] = None)
