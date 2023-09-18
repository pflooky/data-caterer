package com.github.pflooky.datagen.core.generator.delete

import com.datastax.spark.connector.toRDDFunctions
import com.github.pflooky.datacaterer.api.model.Constants.{CASSANDRA_KEYSPACE, CASSANDRA_TABLE}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession}

class CassandraDeleteRecordService extends DeleteRecordService {

  private val LOGGER = Logger.getLogger(getClass.getName)

  override def deleteRecords(dataSourceName: String, trackedRecords: DataFrame, options: Map[String, String])(implicit sparkSession: SparkSession): Unit = {
    val keyspace = options(CASSANDRA_KEYSPACE)
    val table = options(CASSANDRA_TABLE)
    LOGGER.warn(s"Deleting tracked generated records from Cassandra, keyspace=$keyspace, table=$table")
    trackedRecords.rdd.deleteFromCassandra(keyspace, table)
  }

  protected def deleteRecords(dataSourceName: String, trackedRecords: Seq[Row], options: Map[String, String])(implicit sparkSession: SparkSession): Unit = {
    implicit val encoder: Encoder[Row] = Encoders.kryo[Row]
    deleteRecords(dataSourceName, sparkSession.createDataset(trackedRecords), options)
  }
}
