package com.github.pflooky.datagen.core.config

import com.github.pflooky.datagen.core.model.Constants.{APPLICATION_CONFIG_PATH, BASE_FOLDER_PATH, ENABLE_COUNT, ENABLE_DELETE_GENERATED_RECORDS, ENABLE_GENERATE_DATA, ENABLE_GENERATE_PLAN_AND_TASKS, ENABLE_RECORD_TRACKING, FORMAT, PLAN_FILE_PATH, RECORD_TRACKING_FOLDER_PATH, SPARK_MASTER, SUPPORTED_CONNECTION_FORMATS, TASK_FOLDER_PATH}
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.util.Try

trait ConfigParser {

  lazy val config: Config = getConfig
  lazy val baseFolderPath: String = config.getString(BASE_FOLDER_PATH)
  lazy val planFilePath: String = config.getString(PLAN_FILE_PATH)
  lazy val taskFolderPath: String = config.getString(TASK_FOLDER_PATH)
  lazy val enableCount: Boolean = config.getBoolean(ENABLE_COUNT)
  lazy val enableGenerateData: Boolean = config.getBoolean(ENABLE_GENERATE_DATA)
  lazy val enableGeneratePlanAndTasks: Boolean = config.getBoolean(ENABLE_GENERATE_PLAN_AND_TASKS)
  lazy val enableRecordTracking: Boolean = config.getBoolean(ENABLE_RECORD_TRACKING)
  lazy val recordTrackingFolderPath: String = config.getString(RECORD_TRACKING_FOLDER_PATH)
  lazy val enableDeleteGeneratedRecords: Boolean = config.getBoolean(ENABLE_DELETE_GENERATED_RECORDS)
  lazy val sparkMaster: String = config.getString(SPARK_MASTER)
  lazy val connectionConfigsByName: Map[String, Map[String, String]] = getConnectionConfigsByName

  def getConfig: Config = {
    val appConfEnv = System.getenv(APPLICATION_CONFIG_PATH)
    val appConfProp = System.getProperty(APPLICATION_CONFIG_PATH)
    val applicationConfPath = (appConfEnv, appConfProp) match {
      case (null, null) => "application.conf"
      case (env, _) if env != null => env
      case (_, prop) if prop != null => prop
      case _ => "application.conf"
    }
    ConfigFactory.load(applicationConfPath)
  }

  def getConnectionConfigsByName: Map[String, Map[String, String]] = {
    SUPPORTED_CONNECTION_FORMATS.map(format => {
      val tryBaseConfig = Try(config.getConfig(format))
      tryBaseConfig.map(baseConfig => {
        val connectionNames = baseConfig.root().keySet().asScala
        connectionNames.flatMap(name => {
          val connectionConfig = baseConfig.getConfig(name)
          val configValueMap = connectionConfig.entrySet().asScala.map(e => (e.getKey, e.getValue.render().replaceAll("\"", ""))).toMap
          Map(name -> (configValueMap ++ Map(FORMAT -> format)))
        }).toMap
      }).getOrElse(Map())
    }).reduce((x, y) => x ++ y)
  }

}
