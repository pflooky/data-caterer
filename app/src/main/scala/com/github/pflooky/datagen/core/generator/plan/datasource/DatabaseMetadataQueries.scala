package com.github.pflooky.datagen.core.generator.plan.datasource

object DatabaseMetadataQueries {

  lazy val CASSANDRA_TABLE_METADATA_QUERY: String = "SELECT keyspace_name AS schema, table_name AS table FROM system_schema.tables"

  lazy val POSTGRES_TABLE_METADATA_QUERY: String =
    """SELECT table_catalog AS database, table_schema AS schema, table_name AS table
      |FROM information_schema.tables
      |WHERE table_schema != 'pg_catalogs' AND table_schema !='information_schema'
      |""".stripMargin

}
