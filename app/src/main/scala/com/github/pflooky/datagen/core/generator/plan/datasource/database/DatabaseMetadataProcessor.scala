package com.github.pflooky.datagen.core.generator.plan.datasource.database

import org.apache.spark.sql.SparkSession

class DatabaseMetadataProcessor(sparkSession: SparkSession) {

  def getAllDatabaseTables(databaseMetadata: DatabaseMetadata): Array[Map[String, String]] = {
    val allDatabaseSchemasWithTableName = sparkSession.read
      .format(databaseMetadata.format)
      .options(databaseMetadata.connectionConfig ++ databaseMetadata.metadataTable)
      .load()
      .selectExpr(databaseMetadata.selectExpr: _*)
    val optFilterQuery = databaseMetadata.createFilterQuery
    val filteredSchemasAndTables = if (optFilterQuery.isDefined) {
      allDatabaseSchemasWithTableName.filter(optFilterQuery.get)
    } else {
      allDatabaseSchemasWithTableName
    }
    // have to collect here due to being unable to use encoder for DataType and Metadata from Spark. Should be okay given data size is small
    filteredSchemasAndTables.collect()
      .map(r => databaseMetadata.getTableDataOptions(r.getAs[String]("schema"), r.getAs[String]("table")))
  }

  def getAdditionalColumnMetadata(databaseMetadata: DatabaseMetadata): Map[String, String] = {

    Map()
  }
}
