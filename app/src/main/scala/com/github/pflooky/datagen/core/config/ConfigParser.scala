package com.github.pflooky.datagen.core.config

import com.github.pflooky.datagen.core.model.Constants.{APPLICATION_CONFIG_PATH, FORMAT, SPARK_MASTER, SUPPORTED_CONNECTION_FORMATS}
import com.typesafe.config.{Config, ConfigBeanFactory, ConfigFactory, ConfigValueType}
import org.apache.log4j.Logger

import java.io.File
import scala.beans.BeanProperty
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.util.Try

trait ConfigParser {

  private val LOGGER = Logger.getLogger(getClass.getName)

  lazy val config: Config = getConfig
  lazy val flagsConfig: FlagsConfig = ConfigBeanFactory.create(config.getConfig("flags"), classOf[FlagsConfig])
  lazy val foldersConfig: FoldersConfig = ConfigBeanFactory.create(config.getConfig("folders"), classOf[FoldersConfig])
  lazy val metadataConfig: MetadataConfig = ConfigBeanFactory.create(config.getConfig("metadata"), classOf[MetadataConfig])
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
    LOGGER.info(s"Using application config file path, path=$applicationConfPath")
    ConfigFactory.parseFile(new File(applicationConfPath)).resolve()
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
                  @BeanProperty var enableCount: Boolean,
                  @BeanProperty var enableGenerateData: Boolean,
                  @BeanProperty var enableGeneratePlanAndTasks: Boolean,
                  @BeanProperty var enableRecordTracking: Boolean,
                  @BeanProperty var enableDeleteGeneratedRecords: Boolean
                ) {
  def this() = this(true, true, true, true, false)
}

case class FoldersConfig(
                    @BeanProperty var baseFolderPath: String,
                    @BeanProperty var planFilePath: String,
                    @BeanProperty var taskFolderPath: String,
                    @BeanProperty var recordTrackingFolderPath: String
                  ) {
  def this() = this("", "", "", "")
}

case class MetadataConfig(
                           @BeanProperty var numRecordsFromDataSource: Int,
                           @BeanProperty var numRecordsForAnalysis: Int,
                           @BeanProperty var oneOfDistinctCountVsCountThreshold: Double,
                         ) {
  def this() = this(1000, 1000, 0.1)
}