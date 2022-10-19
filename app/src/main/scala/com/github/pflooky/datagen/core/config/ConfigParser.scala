package com.github.pflooky.datagen.core.config

import com.typesafe.config.{Config, ConfigFactory}

import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsScala}

trait ConfigParser {

  private val supportedConnectionConfigurations = List("jdbc", "parquet")  //, "cassandra", "s3"

  lazy val config: Config = getConfig
  lazy val planFilePath: String = config.getString("plan-file-path")
  lazy val taskFolderPath: String = config.getString("task-folder-path")
  lazy val sparkMaster: String = config.getString("spark.master")
  lazy val connectionConfigs: Map[String, Map[String, String]] = getConnectionConfigs

  def getConfig: Config = {
    ConfigFactory.load()
  }

  def getConnectionConfigs: Map[String, Map[String, String]] = {
    supportedConnectionConfigurations.map(s => {
      val baseConfig = config.getConfig(s)
      val baseKey = baseConfig.root().asScala.head._1
      val valueMap = baseConfig.entrySet().asScala
        .map(entry => entry.getKey.replace(s"$baseKey.", "") -> entry.getValue.render().replaceAll("\"", ""))
        .toMap
      Map(baseKey -> (valueMap ++ Map("format" -> s)))
    }).reduce((x, y) => x ++ y)
  }

}
