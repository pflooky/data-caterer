package com.github.pflooky.datagen.core.generator.metadata.datasource

trait MetadataProcessor {

  val dataSourceMetadata: DataSourceMetadata

  def getSubDataSourcesMetadata: Array[Map[String, String]]

}
