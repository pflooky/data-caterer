package com.github.pflooky.datagen.core.generator.plan.datasource.jms

import com.github.pflooky.datagen.core.generator.plan.datasource.DataSourceMetadata
import com.github.pflooky.datagen.core.generator.plan.datasource.database.{ColumnMetadata, ForeignKeyRelationship}
import com.github.pflooky.datagen.core.model.Constants.PATH
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

case class JmsMetadata(name: String, format: String, connectionConfig: Map[String, String]) extends DataSourceMetadata {
  override def getAdditionalColumnMetadata(implicit sparkSession: SparkSession): Dataset[ColumnMetadata] = {
    implicit val encoder: Encoder[ColumnMetadata] = Encoders.kryo[ColumnMetadata]

    sparkSession.emptyDataset
  }

  override def getForeignKeys(implicit sparkSession: SparkSession): Dataset[ForeignKeyRelationship] = {
    implicit val encoder: Encoder[ForeignKeyRelationship] = Encoders.kryo[ForeignKeyRelationship]
    sparkSession.emptyDataset[ForeignKeyRelationship]
  }

  override def toStepName(options: Map[String, String]): String = {
    options(PATH)
  }
}
