package com.github.pflooky.datagen.core.generator.plan.datasource.file

import com.github.pflooky.datagen.core.util.SparkSuite

class FileMetadataProcessorTest extends SparkSuite {
  private val baseFolder = "app/src/test/resources/sample/files"

  test("Can get all distinct folder pathways for csv file type") {
    val fileMetadata = FileMetadata("csv_data", "csv", Map("path" -> baseFolder))
    val fileMetadataProcessor = new FileMetadataProcessor(fileMetadata)

    val result = fileMetadataProcessor.getSubDataSourcesMetadata

    assert(result.length == 2)
    assert(result.forall(m => m("format") == "csv"))
    assert(result.forall(m => m("path").contains(s"$baseFolder/csv/account") || m("path").contains(s"$baseFolder/csv/transactions")))
  }

  test("Can get all distinct folder pathways for parquet file type") {
    val fileMetadata = FileMetadata("parquet_data", "parquet", Map("path" -> baseFolder))
    val fileMetadataProcessor = new FileMetadataProcessor(fileMetadata)

    val result = fileMetadataProcessor.getSubDataSourcesMetadata

    assert(result.length == 3)
    assert(result.forall(m => m("format") == "parquet"))
    assert(result.forall(m =>
      m("path").contains(s"$baseFolder/parquet/account") ||
        m("path").contains(s"$baseFolder/parquet/customer") ||
        m("path").contains(s"$baseFolder/parquet/transactions")))
  }

  test("Can get all distinct folder pathways for json file type") {
    val fileMetadata = FileMetadata("json_data", "json", Map("path" -> baseFolder))
    val fileMetadataProcessor = new FileMetadataProcessor(fileMetadata)

    val result = fileMetadataProcessor.getSubDataSourcesMetadata

    assert(result.length == 2)
    val head = result.head
    assert(result.forall(m => m("format") == "json"))
    assert(result.forall(m => m("path").contains(s"$baseFolder/json") || m("path").contains(s"$baseFolder/csv/json")))
  }

}
