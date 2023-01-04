package com.github.pflooky.datagen.core.generator.plan.datasource

import com.github.pflooky.datagen.core.generator.plan.datasource.DatabaseMetadataQueries.{CASSANDRA_TABLE_METADATA_QUERY, POSTGRES_TABLE_METADATA_QUERY}
import com.github.pflooky.datagen.core.model.Constants.{CASSANDRA, POSTGRES}
import com.github.pflooky.datagen.core.util.SparkProvider

class DataSourceMetadataFactory extends SparkProvider {

  def extractAllDataSourceMetadata(): Unit = {
    connectionConfigs.map(connectionConfig => {
      val query = connectionConfig._1 match {
        case CASSANDRA => Some(CASSANDRA_TABLE_METADATA_QUERY)
        case POSTGRES => Some(POSTGRES_TABLE_METADATA_QUERY)
        case _ => None
      }
      if (query.isDefined) {
        val metadata = sparkSession.sql(query.get)
        //convert to common metadata model
        metadata
      } else {
        //have ot extract out metadata from files
        ""
      }
    })
  }

}
