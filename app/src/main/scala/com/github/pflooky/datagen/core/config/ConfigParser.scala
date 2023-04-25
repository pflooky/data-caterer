package com.github.pflooky.datagen.core.config

import com.github.pflooky.datagen.core.model.Constants.{BASE_FOLDER_PATH, ENABLE_COUNT, ENABLE_GENERATE_PLAN_AND_TASKS, FORMAT, PLAN_FILE_PATH, SPARK_MASTER, SUPPORTED_CONNECTION_CONFIGURATIONS, TASK_FOLDER_PATH}
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsScalaMapConverter}
import scala.util.Try

trait ConfigParser {

  lazy val config: Config = getConfig
  lazy val baseFolderPath: String = config.getString(BASE_FOLDER_PATH)
  lazy val planFilePath: String = config.getString(PLAN_FILE_PATH)
  lazy val taskFolderPath: String = config.getString(TASK_FOLDER_PATH)
  lazy val enableCount: Boolean = config.getBoolean(ENABLE_COUNT)
  lazy val enableGeneratePlanAndTasks: Boolean = config.getBoolean(ENABLE_GENERATE_PLAN_AND_TASKS)
  lazy val sparkMaster: String = config.getString(SPARK_MASTER)
  lazy val connectionConfigsByName: Map[String, Map[String, String]] = getConnectionConfigsByName

  def getConfig: Config = {
    ConfigFactory.load()
  }

  def getConnectionConfigsByName: Map[String, Map[String, String]] = {
    SUPPORTED_CONNECTION_CONFIGURATIONS.map(s => {
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
