package com.github.pflooky.datagen.core.util

import com.github.pflooky.datagen.core.config.ConfigParser
import com.solacesystems.jms.SolMessageProducer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkProvider extends ConfigParser {

  lazy implicit val sparkSession: SparkSession = getSparkSession

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master(sparkMaster)
      .appName("data-caterer")
      .config(baseSparkConfig)
      .config(sparkConnectionConfig)
      .getOrCreate()
  }

}
