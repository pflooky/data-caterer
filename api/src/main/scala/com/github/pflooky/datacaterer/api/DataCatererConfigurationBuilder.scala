package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.connection.{CassandraBuilder, ConnectionTaskBuilder, FileBuilder, HttpBuilder, KafkaBuilder, MySqlBuilder, PostgresBuilder, SolaceBuilder}
import com.github.pflooky.datacaterer.api.model.Constants._
import com.github.pflooky.datacaterer.api.model.DataCatererConfiguration
import com.softwaremill.quicklens.ModifyPimp

case class DataCatererConfigurationBuilder(build: DataCatererConfiguration = DataCatererConfiguration()) {
  def master(master: String): DataCatererConfigurationBuilder =
    this.modify(_.build.master).setTo(master)

  def runtimeConfig(conf: Map[String, String]): DataCatererConfigurationBuilder =
    this.modify(_.build.runtimeConfig)(_ ++ conf)

  def addRuntimeConfig(conf: (String, String)): DataCatererConfigurationBuilder =
    this.modify(_.build.runtimeConfig)(_ ++ Map(conf))


  def connectionConfig(connectionConfigByName: Map[String, Map[String, String]]): DataCatererConfigurationBuilder =
    this.modify(_.build.connectionConfigByName)(_ ++ connectionConfigByName)

  def addConnectionConfig(name: String, format: String, connectionConfig: Map[String, String]): DataCatererConfigurationBuilder =
    this.modify(_.build.connectionConfigByName)(_ ++ Map(name -> (connectionConfig ++ Map(FORMAT -> format))))

  def addConnectionConfig(name: String, format: String, path: String, connectionConfig: Map[String, String]): DataCatererConfigurationBuilder = {
    val pathConf = if (path.nonEmpty) Map(PATH -> path) else Map()
    this.modify(_.build.connectionConfigByName)(_ ++ Map(name -> (connectionConfig ++ Map(FORMAT -> format) ++ pathConf)))
  }

  def csv(name: String, path: String = "", options: Map[String, String] = Map()): DataCatererConfigurationBuilder =
    addConnectionConfig(name, CSV, path, options)

  def parquet(name: String, path: String = "", options: Map[String, String] = Map()): DataCatererConfigurationBuilder =
    addConnectionConfig(name, PARQUET, path, options)

  def orc(name: String, path: String = "", options: Map[String, String] = Map()): DataCatererConfigurationBuilder =
    addConnectionConfig(name, ORC, path, options)

  def json(name: String, path: String = "", options: Map[String, String] = Map()): DataCatererConfigurationBuilder =
    addConnectionConfig(name, JSON, path, options)

  def postgres(
                name: String,
                url: String = DEFAULT_POSTGRES_URL,
                username: String = DEFAULT_POSTGRES_USERNAME,
                password: String = DEFAULT_POSTGRES_PASSWORD,
                options: Map[String, String] = Map()
              ): DataCatererConfigurationBuilder =
    addConnection(name, JDBC, url, username, password, options ++ Map(DRIVER -> POSTGRES_DRIVER))

  def mysql(
             name: String,
             url: String = DEFAULT_MYSQL_URL,
             username: String = DEFAULT_MYSQL_USERNAME,
             password: String = DEFAULT_MYSQL_PASSWORD,
             options: Map[String, String] = Map()
           ): DataCatererConfigurationBuilder =
    addConnection(name, JDBC, url, username, password, options ++ Map(DRIVER -> MYSQL_DRIVER))

  def cassandra(
                 name: String,
                 url: String = DEFAULT_CASSANDRA_URL,
                 username: String = DEFAULT_CASSANDRA_USERNAME,
                 password: String = DEFAULT_CASSANDRA_PASSWORD,
                 options: Map[String, String] = Map()
               ): DataCatererConfigurationBuilder = {
    val sptUrl = url.split(":")
    assert(sptUrl.size == 2, "url should have format '<host>:<port>'")
    val allOptions = Map(
      "spark.cassandra.connection.host" -> sptUrl.head,
      "spark.cassandra.connection.port" -> sptUrl.last,
      "spark.cassandra.auth.username" -> username,
      "spark.cassandra.auth.password" -> password,
    ) ++ options
    addConnectionConfig(name, CASSANDRA, allOptions)
  }

  def jms(name: String, url: String, username: String, password: String, options: Map[String, String] = Map()): DataCatererConfigurationBuilder =
    addConnection(name, JMS, url, username, password, options)

  def solace(
              name: String,
              url: String = DEFAULT_SOLACE_URL,
              username: String = DEFAULT_SOLACE_USERNAME,
              password: String = DEFAULT_SOLACE_PASSWORD,
              vpnName: String = DEFAULT_SOLACE_VPN_NAME,
              connectionFactory: String = DEFAULT_SOLACE_CONNECTION_FACTORY,
              initialContextFactory: String = DEFAULT_SOLACE_INITIAL_CONTEXT_FACTORY,
              options: Map[String, String] = Map()
            ): DataCatererConfigurationBuilder =
    jms(name, url, username, password, Map(
      JMS_VPN_NAME -> vpnName,
      JMS_CONNECTION_FACTORY -> connectionFactory,
      JMS_INITIAL_CONTEXT_FACTORY -> initialContextFactory,
    ) ++ options)

  def kafka(name: String, url: String = DEFAULT_KAFKA_URL, options: Map[String, String] = Map()): DataCatererConfigurationBuilder = {
    addConnectionConfig(name, KAFKA, Map(
      "kafka.bootstrap.servers" -> url,
    ) ++ options)
  }

  def http(name: String, username: String = "", password: String = "", options: Map[String, String] = Map()): DataCatererConfigurationBuilder = {
    val authOptions = if (username.nonEmpty && password.nonEmpty) Map(USERNAME -> username, PASSWORD -> password) else Map()
    addConnectionConfig(name, HTTP, authOptions ++ options)
  }

  private def addConnection(name: String, format: String, url: String, username: String,
                            password: String, options: Map[String, String]): DataCatererConfigurationBuilder = {
    addConnectionConfig(name, format, Map(
      URL -> url,
      USERNAME -> username,
      PASSWORD -> password
    ) ++ options)
  }


  def enableGenerateData(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableGenerateData).setTo(enable)

  def enableCount(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableCount).setTo(enable)

  def enableValidation(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableValidation).setTo(enable)

  def enableFailOnError(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableFailOnError).setTo(enable)

  def enableUniqueCheck(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableUniqueCheck).setTo(enable)

  def enableSaveReports(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableSaveReports).setTo(enable)

  def enableSinkMetadata(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableSinkMetadata).setTo(enable)

  def enableDeleteGeneratedRecords(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableDeleteGeneratedRecords).setTo(enable)

  def enableGeneratePlanAndTasks(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableGeneratePlanAndTasks).setTo(enable)

  def enableRecordTracking(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.build.flagsConfig.enableRecordTracking).setTo(enable)


  def planFilePath(path: String): DataCatererConfigurationBuilder =
    this.modify(_.build.foldersConfig.planFilePath).setTo(path)

  def taskFolderPath(path: String): DataCatererConfigurationBuilder =
    this.modify(_.build.foldersConfig.taskFolderPath).setTo(path)

  def recordTrackingFolderPath(path: String): DataCatererConfigurationBuilder =
    this.modify(_.build.foldersConfig.recordTrackingFolderPath).setTo(path)

  def validationFolderPath(path: String): DataCatererConfigurationBuilder =
    this.modify(_.build.foldersConfig.validationFolderPath).setTo(path)

  def generatedReportsFolderPath(path: String): DataCatererConfigurationBuilder =
    this.modify(_.build.foldersConfig.generatedReportsFolderPath).setTo(path)

  def generatedPlanAndTaskFolderPath(path: String): DataCatererConfigurationBuilder =
    this.modify(_.build.foldersConfig.generatedPlanAndTaskFolderPath).setTo(path)


  def numRecordsFromDataSourceForDataProfiling(numRecords: Int): DataCatererConfigurationBuilder =
    this.modify(_.build.metadataConfig.numRecordsFromDataSource).setTo(numRecords)

  def numRecordsForAnalysisForDataProfiling(numRecords: Int): DataCatererConfigurationBuilder =
    this.modify(_.build.metadataConfig.numRecordsForAnalysis).setTo(numRecords)

  def numGeneratedSamples(numSamples: Int): DataCatererConfigurationBuilder =
    this.modify(_.build.metadataConfig.numGeneratedSamples).setTo(numSamples)

  def oneOfMinCount(minCount: Long): DataCatererConfigurationBuilder =
    this.modify(_.build.metadataConfig.oneOfMinCount).setTo(minCount)

  def oneOfDistinctCountVsCountThreshold(threshold: Double): DataCatererConfigurationBuilder =
    this.modify(_.build.metadataConfig.oneOfDistinctCountVsCountThreshold).setTo(threshold)


  def numRecordsPerBatch(numRecords: Long): DataCatererConfigurationBuilder =
    this.modify(_.build.generationConfig.numRecordsPerBatch).setTo(numRecords)

  def numRecordsPerStep(numRecords: Long): DataCatererConfigurationBuilder =
    this.modify(_.build.generationConfig.numRecordsPerStep).setTo(Some(numRecords))
}

final case class ConnectionConfigWithTaskBuilder(
                                                  dataSourceName: String = DEFAULT_DATA_SOURCE_NAME,
                                                  options: Map[String, String] = Map()
                                                ) {

  def file(name: String, format: String, path: String = "", options: Map[String, String] = Map()): FileBuilder = {
    val configBuilder = DataCatererConfigurationBuilder()
    val fileConnectionConfig = format match {
      case CSV => configBuilder.csv(name, path, options)
      case JSON => configBuilder.json(name, path, options)
      case ORC => configBuilder.orc(name, path, options)
      case PARQUET => configBuilder.parquet(name, path, options)
    }
    setConnectionConfig(name, fileConnectionConfig, FileBuilder())
  }

  def postgres(
            name: String,
            url: String,
            username: String,
            password: String,
            options: Map[String, String] = Map()
          ): PostgresBuilder = {
    val configBuilder = DataCatererConfigurationBuilder().postgres(name, url, username, password, options)
    setConnectionConfig(name, configBuilder, PostgresBuilder())
  }

  def mySql(
            name: String,
            url: String,
            username: String,
            password: String,
            options: Map[String, String] = Map()
          ): MySqlBuilder = {
    val configBuilder = DataCatererConfigurationBuilder().mysql(name, url, username, password, options)
    setConnectionConfig(name, configBuilder, MySqlBuilder())
  }

  def cassandra(
                 name: String,
                 url: String,
                 username: String,
                 password: String,
                 options: Map[String, String] = Map()
               ): CassandraBuilder = {
    val configBuilder = DataCatererConfigurationBuilder().cassandra(name, url, username, password, options)
    setConnectionConfig(name, configBuilder, CassandraBuilder())
  }

  def solace(
              name: String,
              url: String,
              username: String,
              password: String,
              vpnName: String,
              connectionFactory: String,
              initialContextFactory: String,
              options: Map[String, String] = Map()
            ): SolaceBuilder = {
    val configBuilder = DataCatererConfigurationBuilder().solace(name, url, username, password, vpnName, connectionFactory, initialContextFactory, options)
    setConnectionConfig(name, configBuilder, SolaceBuilder())
  }

  def kafka(name: String, url: String, options: Map[String, String] = Map()): KafkaBuilder = {
    val configBuilder = DataCatererConfigurationBuilder().kafka(name, url, options)
    setConnectionConfig(name, configBuilder, KafkaBuilder())
  }

  def http(name: String, username: String, password: String, options: Map[String, String] = Map()): HttpBuilder = {
    val configBuilder = DataCatererConfigurationBuilder().http(name, username, password, options)
    setConnectionConfig(name, configBuilder, HttpBuilder())
  }

  def options(options: Map[String, String]): ConnectionConfigWithTaskBuilder = {
    this.modify(_.options)(_ ++ options)
  }

  private def setConnectionConfig[T <: ConnectionTaskBuilder[_]](name: String, configBuilder: DataCatererConfigurationBuilder, connectionBuilder: T): T = {
    val modifiedConnectionConfig = this.modify(_.dataSourceName).setTo(name)
      .modify(_.options).setTo(configBuilder.build.connectionConfigByName(name))
    connectionBuilder.connectionConfigWithTaskBuilder = modifiedConnectionConfig
    connectionBuilder
  }
}

