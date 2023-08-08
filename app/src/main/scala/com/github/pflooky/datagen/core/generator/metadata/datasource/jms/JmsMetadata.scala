package com.github.pflooky.datagen.core.generator.metadata.datasource.jms

import com.github.pflooky.datagen.core.generator.metadata.datasource.DataSourceMetadata
import com.github.pflooky.datagen.core.generator.metadata.datasource.database.{ColumnMetadata, ForeignKeyRelationship}
import com.github.pflooky.datagen.core.model.Constants.PATH
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

case class JmsMetadata(name: String, format: String, connectionConfig: Map[String, String]) extends DataSourceMetadata {

  override def toStepName(options: Map[String, String]): String = {
    options(PATH)
  }
}
