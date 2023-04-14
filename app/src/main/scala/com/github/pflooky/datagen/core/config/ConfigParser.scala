package com.github.pflooky.datagen.core.config

import com.github.pflooky.datagen.core.model.Constants._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsScalaMapConverter}
import scala.util.Try

trait ConfigParser {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val supportedConnectionConfigurations = List(CSV, JSON, PARQUET, CASSANDRA, JDBC)

  lazy val config: Config = getConfig
  lazy val planFilePath: String = config.getString(PLAN_FILE_PATH)
  lazy val taskFolderPath: String = config.getString(TASK_FOLDER_PATH)
  lazy val enableCount: Boolean = config.getBoolean(ENABLE_COUNT)
  lazy val sparkMaster: String = config.getString(SPARK_MASTER)
  lazy val connectionConfigs: Map[String, Map[String, String]] = getConnectionConfigs

  def getConfig: Config = {
    ConfigFactory.load()
  }

  def getConnectionConfigs: Map[String, Map[String, String]] = {
    LOGGER.info(s"Following data sinks are supported: ${supportedConnectionConfigurations.mkString(",")}")
    supportedConnectionConfigurations.map(s => {
      val tryBaseConfig = Try(config.getConfig(s))
      tryBaseConfig.map(baseConfig => {
        val baseKey = baseConfig.root().asScala.head._1
        val valueMap = baseConfig.entrySet().asScala
          .map(entry => entry.getKey.replace(s"$baseKey.", "") -> entry.getValue.render().replaceAll("\"", ""))
          .toMap
        Map(baseKey -> (valueMap ++ Map(FORMAT -> s)))
      }).getOrElse(Map())
    }).reduce((x, y) => x ++ y)
  }

}
