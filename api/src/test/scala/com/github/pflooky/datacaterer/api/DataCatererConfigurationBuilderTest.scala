package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.model.{FlagsConfig, FoldersConfig, GenerationConfig, MetadataConfig}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DataCatererConfigurationBuilderTest extends AnyFunSuite {

  test("Can create basic configuration with defaults") {
    val result = DataCatererConfigurationBuilder().build

    assert(result.flagsConfig == FlagsConfig())
    assert(result.foldersConfig == FoldersConfig())
    assert(result.metadataConfig == MetadataConfig())
    assert(result.generationConfig == GenerationConfig())
    assert(result.connectionConfigByName.isEmpty)
    assert(result.runtimeConfig.size == 10)
    assert(result.master == "local[*]")
  }

  test("Can create postgres connection configuration") {
    val result = DataCatererConfigurationBuilder()
      .postgres("my_postgres")
      .build
      .connectionConfigByName

    assert(result.size == 1)
    assert(result.contains("my_postgres"))
    val config = result("my_postgres")
    assert(config("url") == "jdbc:postgresql://postgresserver:5432/customer")
    assert(config("user") == "postgres")
    assert(config("password") == "postgres")
    assert(config("format") == "jdbc")
    assert(config("driver") == "org.postgresql.Driver")
  }

  test("Can create postgres connection with custom configuration") {
    val result = DataCatererConfigurationBuilder()
      .postgres("my_postgres", "jdbc:postgresql://localhost:5432/customer", options = Map("stringtype" -> "undefined"))
      .build
      .connectionConfigByName

    assert(result.size == 1)
    assert(result.contains("my_postgres"))
    val config = result("my_postgres")
    assert(config.size == 6)
    assert(config("url") == "jdbc:postgresql://localhost:5432/customer")
    assert(config("stringtype") == "undefined")
  }

  test("Can create mysql connection configuration") {
    val result = DataCatererConfigurationBuilder()
      .mysql("my_mysql")
      .build
      .connectionConfigByName

    assert(result.size == 1)
    assert(result.contains("my_mysql"))
    val config = result("my_mysql")
    assert(config("url") == "jdbc:mysql://mysqlserver:3306/customer")
    assert(config("user") == "root")
    assert(config("password") == "root")
    assert(config("format") == "jdbc")
    assert(config("driver") == "com.mysql.cj.jdbc.Driver")
  }

  test("Can create cassandra connection configuration") {
    val result = DataCatererConfigurationBuilder()
      .cassandra("my_cassandra")
      .build
      .connectionConfigByName

    assert(result.size == 1)
    assert(result.contains("my_cassandra"))
    val config = result("my_cassandra")
    assert(config("spark.cassandra.connection.host") == "cassandraserver")
    assert(config("spark.cassandra.connection.port") == "9042")
    assert(config("spark.cassandra.auth.username") == "cassandra")
    assert(config("spark.cassandra.auth.password") == "cassandra")
    assert(config("format") == "org.apache.spark.sql.cassandra")
  }

  test("Can create solace connection configuration") {
    val result = DataCatererConfigurationBuilder()
      .solace("my_solace")
      .build
      .connectionConfigByName

    assert(result.size == 1)
    assert(result.contains("my_solace"))
    val config = result("my_solace")
    assert(config("url") == "smf://solaceserver:55554")
    assert(config("user") == "admin")
    assert(config("password") == "admin")
    assert(config("format") == "jms")
    assert(config("vpnName") == "default")
    assert(config("connectionFactory") == "/jms/cf/default")
    assert(config("initialContextFactory") == "com.solacesystems.jndi.SolJNDIInitialContextFactory")
  }

  test("Can create kafka connection configuration") {
    val result = DataCatererConfigurationBuilder()
      .kafka("my_kafka")
      .build
      .connectionConfigByName

    assert(result.size == 1)
    assert(result.contains("my_kafka"))
    val config = result("my_kafka")
    assert(config("kafka.bootstrap.servers") == "kafkaserver:9092")
    assert(config("format") == "kafka")
  }

  test("Can create http connection configuration") {
    val result = DataCatererConfigurationBuilder()
      .http("my_http", "user", "pw")
      .build
      .connectionConfigByName

    assert(result.size == 1)
    assert(result.contains("my_http"))
    val config = result("my_http")
    assert(config("user") == "user")
    assert(config("password") == "pw")
  }

  test("Can enable/disable flags") {
    val result = DataCatererConfigurationBuilder()
      .enableCount(false)
      .enableGenerateData(false)
      .enableDeleteGeneratedRecords(true)
      .enableGeneratePlanAndTasks(true)
      .enableUniqueCheck(true)
      .enableFailOnError(false)
      .enableRecordTracking(true)
      .enableSaveReports(true)
      .enableSinkMetadata(true)
      .enableValidation(true)
      .build
      .flagsConfig

    assert(!result.enableCount)
    assert(!result.enableGenerateData)
    assert(result.enableDeleteGeneratedRecords)
    assert(result.enableGeneratePlanAndTasks)
    assert(result.enableUniqueCheck)
    assert(!result.enableFailOnError)
    assert(result.enableRecordTracking)
    assert(result.enableSaveReports)
    assert(result.enableSinkMetadata)
    assert(result.enableValidation)
  }

  test("Can alter folder paths") {
    val result = DataCatererConfigurationBuilder()
      .planFilePath("/my_plan")
      .taskFolderPath("/my_task")
      .recordTrackingFolderPath("/my_record_tracking")
      .validationFolderPath("/my_validation")
      .generatedReportsFolderPath("/my_generation_results")
      .generatedPlanAndTaskFolderPath("/my_generated_plan_tasks")
      .build
      .foldersConfig

    assert(result.planFilePath == "/my_plan")
    assert(result.taskFolderPath == "/my_task")
    assert(result.recordTrackingFolderPath == "/my_record_tracking")
    assert(result.validationFolderPath == "/my_validation")
    assert(result.generatedReportsFolderPath == "/my_generation_results")
    assert(result.generatedPlanAndTaskFolderPath == "/my_generated_plan_tasks")
  }

  test("Can alter metadata configurations") {
    val result = DataCatererConfigurationBuilder()
      .numRecordsFromDataSourceForDataProfiling(1)
      .numRecordsForAnalysisForDataProfiling(2)
      .numGeneratedSamples(3)
      .oneOfMinCount(100)
      .oneOfDistinctCountVsCountThreshold(0.3)
      .build
      .metadataConfig

    assert(result.numRecordsFromDataSource == 1)
    assert(result.numRecordsForAnalysis == 2)
    assert(result.numGeneratedSamples == 3)
    assert(result.oneOfMinCount == 100)
    assert(result.oneOfDistinctCountVsCountThreshold == 0.3)
  }

  test("Can alter generation configurations") {
    val result = DataCatererConfigurationBuilder()
      .numRecordsPerBatch(100)
      .numRecordsPerStep(10)
      .build
      .generationConfig

    assert(result.numRecordsPerBatch == 100)
    assert(result.numRecordsPerStep.contains(10))
  }
}
