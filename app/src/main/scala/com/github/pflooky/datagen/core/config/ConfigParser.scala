package com.github.pflooky.datagen.core.config

import com.github.pflooky.datagen.core.model.Constants.{APPLICATION_CONFIG_PATH, FORMAT, SPARK_MASTER, SUPPORTED_CONNECTION_FORMATS}
import com.github.pflooky.datagen.core.util.ObjectMapperUtil
import com.typesafe.config.{Config, ConfigFactory, ConfigValueType}
import org.apache.log4j.Logger

import java.io.File
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.util.Try

trait ConfigParser {

  private val LOGGER = Logger.getLogger(getClass.getName)

  lazy val config: Config = getConfig
  lazy val flagsConfig: FlagsConfig = ObjectMapperUtil.jsonObjectMapper.convertValue(config.getObject("flags").unwrapped(), classOf[FlagsConfig])
  lazy val foldersConfig: FoldersConfig = ObjectMapperUtil.jsonObjectMapper.convertValue(config.getObject("folders").unwrapped(), classOf[FoldersConfig])
  lazy val metadataConfig: MetadataConfig = ObjectMapperUtil.jsonObjectMapper.convertValue(config.getObject("metadata").unwrapped(), classOf[MetadataConfig])
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
    LOGGER.debug(s"Using application config file path, path=$applicationConfPath")
    val applicationConfFile = new File(applicationConfPath)
    if (!applicationConfFile.exists()) {
      val confFromClassPath = getClass.getClassLoader.getResource(applicationConfPath)
      ConfigFactory.parseURL(confFromClassPath).resolve()
    } else {
      ConfigFactory.parseFile(applicationConfFile).resolve()
    }
  }

  def getConnectionConfigsByName: Map[String, Map[String, String]] = {
    SUPPORTED_CONNECTION_FORMATS.map(format => {
      val tryBaseConfig = Try(config.getConfig(format))
      tryBaseConfig.map(baseConfig => {
        val connectionNames = baseConfig.root().keySet().asScala
        connectionNames.flatMap(name => {
          baseConfig.getValue(name).valueType() match {
            case ConfigValueType.OBJECT =>
              val connectionConfig = baseConfig.getConfig(name)
              val configValueMap = connectionConfig.entrySet().asScala.map(e => (e.getKey, e.getValue.render().replaceAll("\"", ""))).toMap
              Map(name -> (configValueMap ++ Map(FORMAT -> format)))
            case _ => Map[String, Map[String, String]]()
          }
        }).toMap
      }).getOrElse(Map())
    }).reduce((x, y) => x ++ y)
  }

}

case class FlagsConfig(
                        enableCount: Boolean,
                        enableGenerateData: Boolean,
                        enableGeneratePlanAndTasks: Boolean,
                        enableRecordTracking: Boolean,
                        enableDeleteGeneratedRecords: Boolean
                      ) {
  def this() = this(true, true, true, true, false)
}

case class FoldersConfig(
                          baseFolderPath: String,
                          planFilePath: String,
                          taskFolderPath: String,
                          recordTrackingFolderPath: String
                        ) {
  def this() = this("", "", "", "")
}

case class MetadataConfig(
                           numRecordsFromDataSource: Int,
                           numRecordsForAnalysis: Int,
                           oneOfDistinctCountVsCountThreshold: Double,
                           oneOfMinCount: Int = 1000,
                         ) {
  def this() = this(1000, 1000, 0.1, 1000)
}