package com.github.pflooky.datagen.core.util

import com.github.pflooky.datagen.core.config.ConfigParser
import org.apache.spark.sql.SparkSession

trait SparkProvider extends ConfigParser {

  lazy implicit val sparkSession: SparkSession = getSparkSession

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master(sparkMaster)
      .appName("draven-datagen")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .getOrCreate()
  }

}
