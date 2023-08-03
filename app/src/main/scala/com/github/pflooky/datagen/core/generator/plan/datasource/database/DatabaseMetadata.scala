package com.github.pflooky.datagen.core.generator.plan.datasource.database

import com.github.pflooky.datagen.core.generator.plan.datasource.DataSourceMetadata
import com.github.pflooky.datagen.core.model.Constants.{DEFAULT_VALUE, IS_NULLABLE, IS_PRIMARY_KEY, IS_UNIQUE, JDBC, JDBC_QUERY, JDBC_TABLE, MAXIMUM_LENGTH, METADATA_FILTER_SCHEMA, METADATA_FILTER_TABLE, NUMERIC_PRECISION, NUMERIC_SCALE, PRIMARY_KEY_POSITION, SOURCE_COLUMN_DATA_TYPE}
import com.github.pflooky.datagen.core.model.ForeignKeyRelation
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}

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

  override def createFilterQuery: Option[String] = {
    val baseFilterQuery = super.createFilterQuery
    val baseTableFilter = "table_type = 'BASE TABLE'"
    baseFilterQuery.map(f => s"$f AND $baseTableFilter").orElse(Some(baseTableFilter))
  }

  override def toStepName(options: Map[String, String]): String = {
    val dbTable = options(JDBC_TABLE).split("\\.")
    s"${name}_${dbTable.head}_${dbTable.last}"
  }

  override def getForeignKeys(implicit sparkSession: SparkSession): Dataset[ForeignKeyRelationship] = {
    implicit val encoder: Encoder[ForeignKeyRelationship] = Encoders.kryo[ForeignKeyRelationship]
    val filteredForeignKeyData: DataFrame = runQuery(sparkSession, foreignKeyQuery)

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

  override def getAdditionalColumnMetadata(implicit sparkSession: SparkSession): Dataset[ColumnMetadata] = {
    implicit val encoder: Encoder[ColumnMetadata] = Encoders.kryo[ColumnMetadata]
    val filteredTableConstraintData: DataFrame = runQuery(sparkSession, additionalColumnMetadataQuery)

    filteredTableConstraintData.map(r => {
      val isPrimaryKey = if (r.getAs[String]("is_primary_key").equalsIgnoreCase("yes")) "true" else "false"
      val isUnique = if (r.getAs[String]("is_unique").equalsIgnoreCase("yes")) "true" else "false"
      val isNullable = if (r.getAs[String]("is_nullable").equalsIgnoreCase("yes")) "true" else "false"

      val columnMetadata = Map(
        SOURCE_COLUMN_DATA_TYPE -> r.getAs[String]("source_data_type"),
        IS_PRIMARY_KEY -> isPrimaryKey,
        PRIMARY_KEY_POSITION -> r.getAs[String]("primary_key_position"),
        IS_UNIQUE -> isUnique,
        IS_NULLABLE -> isNullable,
        MAXIMUM_LENGTH -> r.getAs[String]("character_maximum_length"),
        NUMERIC_PRECISION -> r.getAs[String]("numeric_precision"),
        NUMERIC_SCALE -> r.getAs[String]("numeric_scale"),
        DEFAULT_VALUE -> r.getAs[String]("column_default")
      ).filter(m => m._2 != null) ++ dataSourceGenerationMetadata(r)

      val dataSourceReadOptions = Map(JDBC_TABLE -> s"${r.getAs[String]("schema")}.${r.getAs("table")}")
      ColumnMetadata(r.getAs("column"), dataSourceReadOptions, columnMetadata)
    })
  }

  /*
    Foreign key query requires the following return columns names to be returned:
    - schema
    - table
    - dbtable
    - column
    - foreign_dbtable
    - foreign_column
     */
  def foreignKeyQuery: String

  /*
  Additional column metadata query requires the following return columns names to be returned:
  - schema
  - table
  - column
  - source_data_type
  - is_nullable
  - character_maximum_length
  - numeric_precision
  - numeric_scale
  - column_default
  - is_unique
  - is_primary_key
  - primary_key_position
   */
  def additionalColumnMetadataQuery: String

  /*
  Given metadata from source database, further metadata could be extracted based on the generation of the columns data
  i.e. auto_increment means we can omit generating the values ourselves, so we add OMIT -> true into the metadata
   */
  def dataSourceGenerationMetadata(row: Row): Map[String, String]

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