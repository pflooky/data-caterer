package com.github.pflooky.datagen.core.util

import com.github.pflooky.datagen.core.config.ConfigParser
import org.apache.spark.sql.SparkSession

trait SparkProvider extends ConfigParser {

  lazy implicit val sparkSession: SparkSession = getSparkSession

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master(sparkMaster)
      .appName("spartagen-datagen")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.planStats.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .config("spark.sql.catalog.postgres", "")
      .config("spark.sql.catalog.cassandra", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .getOrCreate()
  }

}
