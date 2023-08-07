package com.github.pflooky.datagen.core.generator.metadata.datasource.file

import com.github.pflooky.datagen.core.generator.metadata.datasource.{DataSourceMetadata, MetadataProcessor}
import com.github.pflooky.datagen.core.model.Constants.{DELTA, PARQUET, PATH}
import org.apache.spark.sql.SparkSession

class FileMetadataProcessor(override val dataSourceMetadata: DataSourceMetadata)(implicit sparkSession: SparkSession) extends MetadataProcessor {

  /**
   * Given a folder pathway, get all types of files and their corresponding pathways and other connection related metadata
   * that could be used for spark.read.options
   *
   * @return Array of connection config for files
   */
  override def getSubDataSourcesMetadata: Array[Map[String, String]] = {
    val baseFolderPath = dataSourceMetadata.connectionConfig(PATH)
    val fileSuffix = dataSourceMetadata.format match {
      case DELTA => PARQUET
      case x => x
    }
    val df = sparkSession.read
      .option("pathGlobFilter", s"*.$fileSuffix")
      .option("recursiveFileLookup", "true")
      .text(baseFolderPath)
      .selectExpr("input_file_name() AS file_path")
    val filePaths = df.distinct().collect()
      .map(r => r.getAs[String]("file_path"))
    val baseFilePaths = getBaseFolderPathways(filePaths)
    baseFilePaths.map(p => dataSourceMetadata.connectionConfig ++ Map("path" -> p, "format" -> dataSourceMetadata.format))
  }

  private def getBaseFolderPathways(filePaths: Array[String]): Array[String] = {
    val partitionedFiles = filePaths.filter(_.contains("="))
      .map(path => {
        val spt = path.split("/")
        val baseFolderPath = spt.takeWhile(!_.contains("="))
        baseFolderPath.mkString("/")
      }).distinct
    val nonPartitionedFiles = filePaths.filter(!_.contains("="))
      .map(path => {
        val spt = path.split("/")
        val baseFolderPath = spt.slice(0, spt.length - 1)
        baseFolderPath.mkString("/")
      }).distinct
    partitionedFiles ++ nonPartitionedFiles
  }
}
