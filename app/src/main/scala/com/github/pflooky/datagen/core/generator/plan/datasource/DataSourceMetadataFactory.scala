package com.github.pflooky.datagen.core.generator.plan.datasource

import com.github.pflooky.datagen.core.model.Constants.{CASSANDRA, POSTGRES}
import com.github.pflooky.datagen.core.util.SparkProvider

class DataSourceMetadataFactory extends SparkProvider {

  private val databaseMetadata = new DatabaseMetadata(sparkSession)

  def extractAllDataSourceMetadata(): Unit = {
    connectionConfigs.map(connectionConfig => {
      val metadata = connectionConfig._1 match {
        case CASSANDRA => Some(databaseMetadata.getCassandraTableMetadata(connectionConfig._2, List(), List()))
        case POSTGRES => Some(databaseMetadata.getPostgresTableMetadata(connectionConfig._2, List(), List()))
        case _ => None
      }
      metadata
    })
  }

}
