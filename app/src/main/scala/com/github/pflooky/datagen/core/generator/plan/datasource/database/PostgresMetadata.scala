package com.github.pflooky.datagen.core.generator.plan.datasource.database

import com.github.pflooky.datagen.core.model.Constants.{DEFAULT_VALUE, IS_NULLABLE, IS_PRIMARY_KEY, IS_UNIQUE, JDBC_TABLE, MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, PRIMARY_KEY_POSITION, SOURCE_COLUMN_DATA_TYPE}
import com.github.pflooky.datagen.core.model.ForeignKeyRelation
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}

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
    val filteredTableConstraintData: DataFrame = runQuery(sparkSession, query)

    val mappedColumnMetadata = filteredTableConstraintData.map(r => {
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
        |    tc.table_schema                                AS "schema",
        |    tc.table_name                                  AS "table",
        |    concat(tc.table_schema, '.', tc.table_name)    AS dbtable,
        |    kcu.column_name                                AS "column",
        |    concat(ccu.table_schema, '.', ccu.table_name)  AS foreign_dbtable,
        |    ccu.column_name                                AS foreign_column
        |FROM
        |    information_schema.table_constraints AS tc
        |        JOIN information_schema.key_column_usage AS kcu
        |             ON tc.constraint_name = kcu.constraint_name
        |                 AND tc.table_schema = kcu.table_schema
        |        JOIN information_schema.constraint_column_usage AS ccu
        |             ON ccu.constraint_name = tc.constraint_name
        |                 AND ccu.table_schema = tc.table_schema
        |WHERE tc.constraint_type = 'FOREIGN KEY'""".stripMargin
    val filteredForeignKeyData: DataFrame = runQuery(sparkSession, query)

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
