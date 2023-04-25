package com.github.pflooky.datagen.core.generator.plan.datasource.database

import com.github.pflooky.datagen.core.generator.plan.datasource.DataSourceMetadata
import com.github.pflooky.datagen.core.model.Constants.{CASSANDRA, CASSANDRA_KEYSPACE, CASSANDRA_TABLE, COLUMN_SOURCE_DATA_TYPE, DEFAULT_VALUE, IS_NULLABLE, IS_PRIMARY_KEY, IS_UNIQUE, JDBC, JDBC_QUERY, JDBC_TABLE, MAXIMUM_LENGTH, METADATA_FILTER_SCHEMA, METADATA_FILTER_TABLE, NUMERIC_PRECISION, NUMERIC_SCALE, PRIMARY_KEY_POSITION}
import com.github.pflooky.datagen.core.model.ForeignKeyRelation
import org.apache.spark.sql.functions.{col, collect_list, concat, lit}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, functions}

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
}

case class PostgresMetadata(name: String, connectionConfig: Map[String, String]) extends JdbcMetadata {
  override val baseFilterSchema: List[String] = List("pg_catalog", "information_schema")

  override def getAdditionalColumnMetadata(implicit sparkSession: SparkSession): Dataset[ColumnMetadata] = {
    implicit val encoder: Encoder[ColumnMetadata] = Encoders.kryo[ColumnMetadata]
    val query =
      """SELECT c.table_schema                                                        AS "schema",
        |       c.table_name                                                          AS "table",
        |       c.column_name                                                         AS "column",
        |       c.data_type                                                           AS source_data_type,
        |       c.is_nullable,
        |       c.character_maximum_length,
        |       c.numeric_precision,
        |       c.numeric_precision_radix,
        |       c.numeric_scale,
        |       c.column_default,
        |       CASE WHEN uc.constraint_type = 'UNIQUE' THEN 'YES' ELSE 'NO' END      AS is_unique,
        |       CASE WHEN pc.constraint_type = 'PRIMARY KEY' THEN 'YES' ELSE 'NO' END AS is_primary_key,
        |       pc.ordinal_position                                                   AS primary_key_position
        |FROM information_schema.columns AS c
        |         LEFT OUTER JOIN (SELECT tc.constraint_name, tc.constraint_type, tc.table_schema, tc.table_name, ccu.column_name
        |                          FROM information_schema.table_constraints tc
        |                                   JOIN information_schema.constraint_column_usage ccu
        |                                        ON tc.constraint_name = ccu.constraint_name AND
        |                                           tc.table_schema = ccu.table_schema AND tc.table_name = ccu.table_name
        |                          WHERE constraint_type = 'UNIQUE') uc
        |                         ON uc.column_name = c.column_name AND uc.table_schema = c.table_schema AND uc.table_name = c.table_name
        |         LEFT OUTER JOIN (SELECT tc1.constraint_name, tc1.constraint_type, tc1.table_schema, tc1.table_name, kcu.column_name, kcu.ordinal_position
        |                          FROM information_schema.table_constraints tc1
        |                                   JOIN information_schema.key_column_usage kcu
        |                                        ON tc1.constraint_name = kcu.constraint_name AND
        |                                           tc1.table_schema = kcu.table_schema AND tc1.table_name = kcu.table_name
        |                          WHERE constraint_type = 'PRIMARY KEY') pc
        |                         ON pc.column_name = c.column_name AND pc.table_schema = c.table_schema AND pc.table_name = c.table_name""".stripMargin
    val tableConstraintData = sparkSession.read
      .format(JDBC)
      .options(connectionConfig ++ Map(JDBC_QUERY -> query))
      .load()
    val optCreateFilterQuery = createFilterQuery
    val filteredTableConstraintData = if (optCreateFilterQuery.isDefined) {
      tableConstraintData.filter(optCreateFilterQuery.get)
    } else tableConstraintData

    val mappedColumnMetadata = filteredTableConstraintData.map(r => {
      val isPrimaryKey = if (r.getAs[String]("is_primary_key").equalsIgnoreCase("yes")) "true" else "false"
      val isUnique = if (r.getAs[String]("is_unique").equalsIgnoreCase("yes")) "true" else "false"
      val isNullable = if (r.getAs[String]("is_nullable").equalsIgnoreCase("yes")) "true" else "false"

      val columnMetadata = Map(
        COLUMN_SOURCE_DATA_TYPE -> r.getAs[String]("source_data_type"),
        IS_PRIMARY_KEY -> isPrimaryKey,
        PRIMARY_KEY_POSITION -> r.getAs[String]("primary_key_position"),
        IS_UNIQUE -> isUnique,
        IS_NULLABLE -> isNullable,
        MAXIMUM_LENGTH -> r.getAs[String]("character_maximum_length"),
        NUMERIC_PRECISION -> r.getAs[String]("numeric_precision"),
        NUMERIC_SCALE -> r.getAs[String]("numeric_scale"),
        DEFAULT_VALUE -> r.getAs[String]("column_default")
      ).filter(m => m._2 != null)

      val dataSourceReadOptions = Map(JDBC_TABLE -> s"${r.getAs[String]("schema")}.${r.getAs("table")}")
      ColumnMetadata(r.getAs("column"), dataSourceReadOptions, columnMetadata)
    })
    mappedColumnMetadata
  }

  override def getForeignKeys(implicit sparkSession: SparkSession): Dataset[ForeignKeyRelationship] = {
    implicit val encoder: Encoder[ForeignKeyRelationship] = Encoders.kryo[ForeignKeyRelationship]
    val query =
      """SELECT
        |    tc.constraint_name,
        |    tc.table_schema AS "schema",
        |    tc.table_name AS "table",
        |    concat(tc.table_schema, '.', tc.table_name) AS dbtable,
        |    kcu.column_name AS "column",
        |    concat(ccu.table_schema, '.', ccu.table_name) AS foreign_dbtable,
        |    ccu.column_name AS foreign_column
        |FROM
        |    information_schema.table_constraints AS tc
        |        JOIN information_schema.key_column_usage AS kcu
        |             ON tc.constraint_name = kcu.constraint_name
        |                 AND tc.table_schema = kcu.table_schema
        |        JOIN information_schema.constraint_column_usage AS ccu
        |             ON ccu.constraint_name = tc.constraint_name
        |                 AND ccu.table_schema = tc.table_schema
        |WHERE tc.constraint_type = 'FOREIGN KEY'""".stripMargin
    val foreignKeyData = sparkSession.read
      .format(JDBC)
      .options(connectionConfig ++ Map(JDBC_QUERY -> query))
      .load()
    val optCreateFilterQuery = createFilterQuery
    val filteredForeignKeyData = if (optCreateFilterQuery.isDefined) {
      foreignKeyData.filter(optCreateFilterQuery.get)
    } else foreignKeyData

    filteredForeignKeyData.map(r =>
      ForeignKeyRelationship(
        ForeignKeyRelation(name,
          toStepName(Map(JDBC_TABLE -> r.getAs[String]("foreign_dbtable"))),
          r.getAs[String]("foreign_column")
        ),
        ForeignKeyRelation(name,
          toStepName(Map(JDBC_TABLE -> r.getAs[String]("dbtable"))),
          r.getAs[String]("column")
        )
      )
    )
  }
}

case class CassandraMetadata(name: String, connectionConfig: Map[String, String]) extends DatabaseMetadata {
  override val format: String = CASSANDRA

  override val metadataTable: Map[String, String] = Map(CASSANDRA_KEYSPACE -> "system_schema", CASSANDRA_TABLE -> "tables")

  override val selectExpr: List[String] = List("keyspace_name AS schema", "table_name AS table")

  override def getTableDataOptions(schema: String, table: String): Map[String, String] = {
    Map(CASSANDRA_KEYSPACE -> schema, CASSANDRA_TABLE -> table)
  }

  override def getAdditionalColumnMetadata(implicit sparkSession: SparkSession): Dataset[ColumnMetadata] = {
    ???
  }

  override def getForeignKeys(implicit sparkSession: SparkSession): Dataset[ForeignKeyRelationship] = ???

  override def toStepName(options: Map[String, String]): String = {
    s"${name}_${options(CASSANDRA_KEYSPACE)}_${options(CASSANDRA_TABLE)}"
  }
}

case class ColumnMetadata(column: String, dataSourceReadOptions: Map[String, String], metadata: Map[String, String])

case class ForeignKeyRelationship(foreignKeyRelation: ForeignKeyRelation, foreignKey: ForeignKeyRelation)