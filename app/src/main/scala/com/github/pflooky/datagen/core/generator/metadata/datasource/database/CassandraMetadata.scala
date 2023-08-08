package com.github.pflooky.datagen.core.generator.metadata.datasource.database

import com.github.pflooky.datagen.core.model.Constants.{CASSANDRA, CASSANDRA_KEYSPACE, CASSANDRA_TABLE, CLUSTERING_ORDER, IS_NULLABLE, IS_PRIMARY_KEY, PRIMARY_KEY_POSITION, SOURCE_COLUMN_DATA_TYPE}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, desc, row_number}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

case class CassandraMetadata(name: String, connectionConfig: Map[String, String]) extends DatabaseMetadata {

  override val format: String = CASSANDRA

  override val metadataTable: Map[String, String] = Map(CASSANDRA_KEYSPACE -> "system_schema", CASSANDRA_TABLE -> "tables")

  override val selectExpr: List[String] = List("keyspace_name AS schema", "table_name AS table", "'BASE TABLE' AS table_type")

  override val baseFilterSchema: List[String] = List("system", "system_traces", "system_auth", "system_schema", "system_distributed", "system_backups",
    "dse_security", "dse_system_local", "dse_system", "dse_leases", "dse_insights", "dse_insights_local", "dse_perf", "solr_admin")

  override def getTableDataOptions(schema: String, table: String): Map[String, String] = {
    Map(CASSANDRA_KEYSPACE -> schema, CASSANDRA_TABLE -> table)
  }

  override def getAdditionalColumnMetadata(implicit sparkSession: SparkSession): Dataset[ColumnMetadata] = {
    val cassandraColumnMetadata = sparkSession
      .read
      .format(CASSANDRA)
      .options(connectionConfig)
      .options(Map(CASSANDRA_KEYSPACE -> "system_schema", CASSANDRA_TABLE -> "columns"))
      .load()
      .selectExpr("keyspace_name AS schema", "table_name AS table", "'BASE TABLE' AS table_type", "column_name AS column",
        "type AS source_data_type", "clustering_order", "kind AS primary_key_type", "position AS primary_key_position")
    val filteredMetadata = createFilterQuery.map(cassandraColumnMetadata.filter).getOrElse(cassandraColumnMetadata)

    val metadataWithPrimaryKeyPosition = filteredMetadata
      .filter("primary_key_type != 'regular'")
      .select(col("schema"), col("table"), col("column"), col("primary_key_type"),
        col("source_data_type"), col("clustering_order"),
        row_number()
          .over(
            Window.partitionBy("schema", "table")
              .orderBy(desc("primary_key_type"), asc("primary_key_position"))
          ).as("primary_key_position")
      )

    metadataWithPrimaryKeyPosition
      .map(r => {
        val isPrimaryKey = if (r.getAs[String]("primary_key_type") != "regular") "true" else "false"
        val isNullable = if (isPrimaryKey.equalsIgnoreCase("true")) "false" else "true"
        val clusteringOrder = r.getAs[String]("clustering_order")

        val metadata = Map(
          SOURCE_COLUMN_DATA_TYPE -> r.getAs[String]("source_data_type"),
          IS_PRIMARY_KEY -> isPrimaryKey,
          PRIMARY_KEY_POSITION -> r.getAs[String]("primary_key_position"),
          IS_NULLABLE -> isNullable,
        )
        val metadataWithClusterOrder = if (clusteringOrder != "none") metadata ++ Map(CLUSTERING_ORDER -> clusteringOrder) else metadata

        val dataSourceReadOptions = Map(CASSANDRA_KEYSPACE -> r.getAs[String]("schema"), CASSANDRA_TABLE -> r.getAs[String]("table"))
        ColumnMetadata(r.getAs[String]("column"), dataSourceReadOptions, metadataWithClusterOrder)
      })
  }

  override def getForeignKeys(implicit sparkSession: SparkSession): Dataset[ForeignKeyRelationship] = {
    sparkSession.emptyDataset[ForeignKeyRelationship]
  }

  override def toStepName(options: Map[String, String]): String = {
    s"${name}_${options(CASSANDRA_KEYSPACE)}_${options(CASSANDRA_TABLE)}"
  }
}
