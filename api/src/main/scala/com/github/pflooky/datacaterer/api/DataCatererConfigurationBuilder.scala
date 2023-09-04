package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.model.Constants._
import com.github.pflooky.datacaterer.api.model.DataCatererConfiguration
import com.softwaremill.quicklens.ModifyPimp

case class DataCatererConfigurationBuilder(dataCatererConfiguration: DataCatererConfiguration = DataCatererConfiguration()) {

  def sparkMaster(master: String): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.sparkMaster).setTo(master)

  def sparkConfig(conf: Map[String, String]): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.sparkConfig)(_ ++ conf)

  def addSparkConfig(conf: (String, String)): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.sparkConfig)(_ ++ Map(conf))


  def connectionConfig(connectionConfigByName: Map[String, Map[String, String]]): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.connectionConfigByName)(_ ++ connectionConfigByName)

  def addConnectionConfig(name: String, format: String, connectionConfig: Map[String, String]): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.connectionConfigByName)(_ ++ Map(name -> (connectionConfig ++ Map(FORMAT -> format))))

  def addConnectionConfig(name: String, format: String, path: String, connectionConfig: Map[String, String]): DataCatererConfigurationBuilder = {
    val pathConf = if (path.nonEmpty) Map(PATH -> path) else Map()
    this.modify(_.dataCatererConfiguration.connectionConfigByName)(_ ++ Map(name -> (connectionConfig ++ Map(FORMAT -> format) ++ pathConf)))
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
                url: String = "jdbc:postgresql://postgresserver:5432/customer",
                username: String = "postgres",
                password: String = "postgres",
                options: Map[String, String] = Map()
              ): DataCatererConfigurationBuilder =
    addConnection(name, JDBC, url, username, password, options ++ Map(DRIVER -> POSTGRES_DRIVER))

  def mysql(
             name: String,
             url: String = "jdbc:mysql://mysqlserver:3306/customer",
             username: String = "root",
             password: String = "root",
             options: Map[String, String] = Map()
           ): DataCatererConfigurationBuilder =
    addConnection(name, JDBC, url, username, password, options ++ Map(DRIVER -> MYSQL_DRIVER))

  def cassandra(
                 name: String,
                 url: String = "cassandraserver:9042",
                 username: String = "cassandra",
                 password: String = "cassandra",
                 options: Map[String, String] = Map()
               ): DataCatererConfigurationBuilder = {
    val sptUrl = url.split(":")
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
              url: String = "smf://solaceserver:55554",
              username: String = "admin",
              password: String = "admin",
              vpnName: String = "default",
              connectionFactory: String = "/jms/cf/default",
              initialContextFactory: String = "com.solacesystems.jndi.SolJNDIInitialContextFactory",
              options: Map[String, String] = Map()
            ): DataCatererConfigurationBuilder =
    jms(name, url, username, password, Map(
      JMS_VPN_NAME -> vpnName,
      JMS_CONNECTION_FACTORY -> connectionFactory,
      JMS_INITIAL_CONTEXT_FACTORY -> initialContextFactory,
    ) ++ options)

  def kafka(name: String, url: String = "kafkaserver:9092", options: Map[String, String] = Map()): DataCatererConfigurationBuilder = {
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
    this.modify(_.dataCatererConfiguration.flagsConfig.enableGenerateData).setTo(enable)

  def enableCount(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.flagsConfig.enableCount).setTo(enable)

  def enableValidation(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.flagsConfig.enableValidation).setTo(enable)

  def enableFailOnError(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.flagsConfig.enableFailOnError).setTo(enable)

  def enableUniqueCheck(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.flagsConfig.enableUniqueCheck).setTo(enable)

  def enableSaveReports(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.flagsConfig.enableSaveReports).setTo(enable)

  def enableSinkMetadata(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.flagsConfig.enableSinkMetadata).setTo(enable)

  def enableDeleteGeneratedRecords(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.flagsConfig.enableDeleteGeneratedRecords).setTo(enable)

  def enableGeneratePlanAndTasks(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.flagsConfig.enableGeneratePlanAndTasks).setTo(enable)

  def enableRecordTracking(enable: Boolean): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.flagsConfig.enableRecordTracking).setTo(enable)


  def planFilePath(path: String): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.foldersConfig.planFilePath).setTo(path)

  def taskFolderPath(path: String): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.foldersConfig.taskFolderPath).setTo(path)

  def recordTrackingFolderPath(path: String): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.foldersConfig.recordTrackingFolderPath).setTo(path)

  def validationFolderPath(path: String): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.foldersConfig.validationFolderPath).setTo(path)

  def generatedReportsFolderPath(path: String): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.foldersConfig.generatedReportsFolderPath).setTo(path)

  def generatedPlanAndTaskFolderPath(path: String): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.foldersConfig.generatedPlanAndTaskFolderPath).setTo(path)


  def numRecordsFromDataSourceForDataProfiling(numRecords: Int): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.metadataConfig.numRecordsFromDataSource).setTo(numRecords)

  def numRecordsForAnalysisForDataProfiling(numRecords: Int): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.metadataConfig.numRecordsForAnalysis).setTo(numRecords)

  def numGeneratedSamples(numSamples: Int): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.metadataConfig.numGeneratedSamples).setTo(numSamples)

  def oneOfMinCount(minCount: Int): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.metadataConfig.oneOfMinCount).setTo(minCount)

  def oneOfDistinctCountVsCountThreshold(threshold: Double): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.metadataConfig.oneOfDistinctCountVsCountThreshold).setTo(threshold)


  def numRecordsPerBatch(numRecords: Long): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.generationConfig.numRecordsPerBatch).setTo(numRecords)

  def numRecordsPerStep(numRecords: Long): DataCatererConfigurationBuilder =
    this.modify(_.dataCatererConfiguration.generationConfig.numRecordsPerStep).setTo(Some(numRecords))
}
