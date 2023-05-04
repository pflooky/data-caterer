package com.github.pflooky.datagen.core.generator.plan.datasource.database

import com.github.pflooky.datagen.core.generator.plan.datasource.DataSourceMetadata
import com.github.pflooky.datagen.core.model.Constants.{CASSANDRA, CASSANDRA_KEYSPACE, CASSANDRA_TABLE, DEFAULT_VALUE, IS_NULLABLE, IS_PRIMARY_KEY, IS_UNIQUE, JDBC, JDBC_QUERY, JDBC_TABLE, MAXIMUM_LENGTH, METADATA_FILTER_SCHEMA, METADATA_FILTER_TABLE, NUMERIC_PRECISION, NUMERIC_SCALE, PRIMARY_KEY_POSITION, SOURCE_COLUMN_DATA_TYPE, SOURCE_MAXIMUM_LENGTH}
import com.github.pflooky.datagen.core.model.ForeignKeyRelation
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}

trait DatabaseMetadata extends DataSourceMetadata {
  val metadataTable: Map[String, String]
  val selectExpr: List[String]
  val baseFilterSchema: List[String] = List()
  val baseFilterTable: List[String] = List()

  def getTableDataOptions(schema: String, table: String): Map[String, String]

  def createFilterQuery: Option[String] = {
    val filterSchema = if (connectionConfig.contains(METADATA_FILTER_SCHEMA)) {
      val configFilterSchema = connectionConfig(METADATA_FILTER_SCHEMA).split(",").map(_.trim).toList
      configFilterSchema ++ baseFilterSchema
    } else baseFilterSchema
    val filterTable = if (connectionConfig.contains(METADATA_FILTER_TABLE)) {
      val configFilterTable = connectionConfig(METADATA_FILTER_TABLE).split(",").map(_.trim).toList
      configFilterTable ++ baseFilterTable
    } else baseFilterTable

    val filterSchemaQuery = filterSchema.map(s => s"schema != '$s'").mkString(" AND ")
    val filterTableQuery = filterTable.map(t => s"table != '$t'").mkString(" AND ")
    (filterSchemaQuery.nonEmpty, filterTableQuery.nonEmpty) match {
      case (true, true) => Some(s"$filterSchemaQuery AND $filterTableQuery")
      case (true, false) => Some(filterSchemaQuery)
      case (false, true) => Some(filterTableQuery)
      case _ => None
    }
  }
}

trait JdbcMetadata extends DatabaseMetadata {
  override val format: String = JDBC

  override val metadataTable: Map[String, String] = Map(JDBC_TABLE -> "information_schema.tables")

  override val selectExpr: List[String] = List("table_schema AS schema", "table_name AS table")

  override def getTableDataOptions(schema: String, table: String): Map[String, String] = {
    Map(JDBC_TABLE -> s"$schema.$table")
  }

  override def toStepName(options: Map[String, String]): String = {
    val dbTable = options(JDBC_TABLE).split("\\.")
    s"${name}_${dbTable.head}_${dbTable.last}"
  }

  def runQuery(sparkSession: SparkSession, query: String): DataFrame = {
    val queryData = sparkSession.read
      .format(JDBC)
      .options(connectionConfig ++ Map(JDBC_QUERY -> query))
      .load()
    val optCreateFilterQuery = createFilterQuery
    if (optCreateFilterQuery.isDefined) {
      queryData.filter(optCreateFilterQuery.get)
    } else {
      queryData
    }
  }
}

case class ColumnMetadata(column: String, dataSourceReadOptions: Map[String, String], metadata: Map[String, String])

case class ForeignKeyRelationship(key: ForeignKeyRelation, foreignKey: ForeignKeyRelation)