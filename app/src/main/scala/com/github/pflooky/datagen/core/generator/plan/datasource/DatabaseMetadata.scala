package com.github.pflooky.datagen.core.generator.plan.datasource

import com.github.pflooky.datagen.core.model.Constants._
import com.github.pflooky.datagen.core.model.Task
import com.github.pflooky.datagen.core.util.MetadataUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class DatabaseMetadata(sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def getCassandraTableMetadata(connectionConfig: (String, Map[String, String]),
                                filterSchema: List[String],
                                filterTable: List[String]): Task = {
    val options = connectionConfig._2 ++ Map(
      CASSANDRA_KEYSPACE -> "system_schema",
      CASSANDRA_TABLE -> "tables"
    )
    val selectExpr = List("keyspace_name AS schema", "table_name AS table")
    val structTypes = getMetadata(CASSANDRA, options, filterSchema, filterTable, selectExpr)
    val task = Task.fromMetadata(connectionConfig._1, CASSANDRA, structTypes)
    task
  }

  def getPostgresTableMetadata(connectionConfig: (String, Map[String, String]),
                               filterSchema: List[String],
                               filterTable: List[String]): Task = {
    val options = connectionConfig._2 ++ Map(JDBC_TABLE -> "information_schema.tables")
    val selectExpr = List("table_schema AS schema", "table_name AS table")
    val filterSchemaWithDefaults = filterSchema ++ List("pg_catalog", "information_schema")
    val structTypes = getMetadata(JDBC, options, filterSchemaWithDefaults, filterTable, selectExpr)
    val task = Task.fromMetadata(connectionConfig._1, POSTGRES, structTypes)
    task
  }

  private def getMetadata(dbType: String, connectionConfig: Map[String, String],
                          filterSchema: List[String], filterTable: List[String],
                          selectExpr: List[String]): List[(Map[String, String], StructType)] = {
    LOGGER.info(s"Extracting out metadata from database, db-type=$dbType")
    val schemasWithTable = sparkSession.read.format(dbType).options(connectionConfig).load()
      .selectExpr(selectExpr: _*)
    val filterQuery = createFilterQuery(filterSchema, filterTable)
    val filteredSchemasWithTable = if (filterQuery.isDefined) {
      schemasWithTable.filter(filterQuery.get)
    } else {
      schemasWithTable
    }

    // have to collect here due to being unable to use encoder for DataType and Metadata from Spark. Should be okay given data size is small
    val tableMetadata = filteredSchemasWithTable.collect().map(r => {
      val dbTable = DatabaseTable(r.getAs[String]("schema"), r.getAs[String]("table"))
      val readTableOptions = dbType match {
        case JDBC => Map(JDBC_TABLE -> s"${dbTable.schema}.${dbTable.table}")
        case CASSANDRA => Map(
          CASSANDRA_KEYSPACE -> dbTable.schema,
          CASSANDRA_TABLE -> dbTable.table
        )
      }
      //TODO: Will this load in all the data? Will probably need to look to make configurable value to define how many rows to get metadata from
      val tableData = sparkSession.read.format(dbType).options(connectionConfig ++ readTableOptions).load()
      val fieldsWithMetadata = MetadataUtil.getFieldMetadata(tableData)

      (readTableOptions, StructType(fieldsWithMetadata))
    })
    tableMetadata.toList
  }

  private def createFilterQuery(filterSchema: List[String], filterTable: List[String]): Option[String] = {
    val schemaQuery = if (filterSchema.nonEmpty) {
      filterSchema.map(s => s"schema != '$s'").mkString(" AND ")
    } else ""
    val tableQuery = if (filterTable.nonEmpty) {
      filterTable.map(t => s"table != '$t'").mkString(" AND ")
    } else ""
    (filterSchema.nonEmpty, filterTable.nonEmpty) match {
      case (true, true) => Some(s"$schemaQuery AND $tableQuery")
      case (true, false) => Some(schemaQuery)
      case (false, true) => Some(tableQuery)
      case _ => None
    }

  }
}

case class DatabaseTable(schema: String, table: String)

