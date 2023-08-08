package com.github.pflooky.datagen.core.generator.metadata.datasource.database

import com.github.pflooky.datagen.core.generator.metadata.datasource.{DataSourceMetadata, MetadataProcessor}
import org.apache.spark.sql.SparkSession

class DatabaseMetadataProcessor(override val dataSourceMetadata: DataSourceMetadata)(implicit sparkSession: SparkSession) extends MetadataProcessor {

  override def getSubDataSourcesMetadata: Array[Map[String, String]] = {
    val databaseMetadata: DatabaseMetadata = dataSourceMetadata match {
      case dm: DatabaseMetadata => dm
      case _ => throw new RuntimeException("Expecting data source metadata to be of type DatabaseMetadata to extract sub data sources metadata")
    }

    val allDatabaseSchemasWithTableName = sparkSession.read
      .format(databaseMetadata.format)
      .options(databaseMetadata.connectionConfig ++ databaseMetadata.metadataTable)
      .load()
      .selectExpr(databaseMetadata.selectExpr: _*)
    val baseTableFilter = "table_type = 'BASE TABLE'"
    val filteredSchemasAndTables = databaseMetadata.createFilterQuery
      .map(f => s"$f AND $baseTableFilter")
      .orElse(Some(baseTableFilter))
      .map(fq => allDatabaseSchemasWithTableName.filter(fq))
      .getOrElse(allDatabaseSchemasWithTableName)
    // have to collect here due to being unable to use encoder for DataType and Metadata from Spark. Should be okay given data size is small
    filteredSchemasAndTables.collect()
      .map(r => databaseMetadata.getTableDataOptions(r.getAs[String]("schema"), r.getAs[String]("table")))
  }
}
