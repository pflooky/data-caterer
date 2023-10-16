package com.github.pflooky.datagen.core.generator.metadata.datasource.file

import com.github.pflooky.datagen.core.util.SparkSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FileMetadataTest extends SparkSuite {
  private val baseFolder = "src/test/resources/sample/files"

  test("Can get all distinct folder pathways for csv file type") {
    val fileMetadata = FileMetadata("csv_data", "csv", Map("path" -> baseFolder))

    val result = fileMetadata.getSubDataSourcesMetadata

    assert(result.length == 2)
    assert(result.forall(m => m.readOptions("format") == "csv"))
    assert(result.forall(m => m.readOptions("path").contains(s"$baseFolder/csv/account") || m.readOptions("path").contains(s"$baseFolder/csv/transactions")))
  }

  test("Can get all distinct folder pathways for parquet file type") {
    val fileMetadata = FileMetadata("parquet_data", "parquet", Map("path" -> baseFolder))

    val result = fileMetadata.getSubDataSourcesMetadata

    assert(result.length == 3)
    assert(result.forall(m => m.readOptions("format") == "parquet"))
    assert(result.forall(m =>
      m.readOptions("path").contains(s"$baseFolder/parquet/account") ||
        m.readOptions("path").contains(s"$baseFolder/parquet/customer") ||
        m.readOptions("path").contains(s"$baseFolder/parquet/transactions")))
  }

  test("Can get all distinct folder pathways for json file type") {
    val fileMetadata = FileMetadata("json_data", "json", Map("path" -> baseFolder))

    val result = fileMetadata.getSubDataSourcesMetadata

    assert(result.length == 2)
    assert(result.forall(m => m.readOptions("format") == "json"))
    assert(result.forall(m => m.readOptions("path").contains(s"$baseFolder/json") || m.readOptions("path").contains(s"$baseFolder/csv/json")))
  }

}
