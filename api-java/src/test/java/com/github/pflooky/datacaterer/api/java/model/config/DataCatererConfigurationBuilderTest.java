package com.github.pflooky.datacaterer.api.java.model.config;

import com.github.pflooky.datacaterer.api.model.Constants;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DataCatererConfigurationBuilderTest {

    @Test
    public void canCreateDataCatererConfigurationWithConnections() {
        var result = new DataCatererConfigurationBuilder()
                .master("local[1]")
                .runtimeConfig(Map.of("spark.conf", "my_value"))
                .addRuntimeConfig("spark.new.conf", "second_val")
                .csv("my_csv", "/path/csv")
                .parquet("my_parquet", "/path/parquet")
                .orc("my_orc", "/path/orc")
                .json("my_json", "/path/json")
                .postgres("my_postgres", "jdbc:postgresql://localhost:5432")
                .mysql("my_mysql", "jdbc:mysql://localhost:3306")
                .cassandra("my_cassandra", "localhost:9042")
                .jms("my_jms", "localhost:1234", "admin", "admin", Collections.emptyMap())
                .solace("my_solace", "smf://localhost:55554")
                .kafka("my_kafka", "localhost:9092")
                .http("my_http")
                .configuration().build();

        assertEquals(11, result.connectionConfigByName().size());
        assertTrue(result.connectionConfigByName().contains("my_csv"));
        assertTrue(result.connectionConfigByName().get("my_csv").get().get(Constants.FORMAT()).contains(Constants.CSV()));
        assertTrue(result.connectionConfigByName().get("my_csv").get().get(Constants.PATH()).contains("/path/csv"));
        assertTrue(result.connectionConfigByName().contains("my_parquet"));
        assertTrue(result.connectionConfigByName().get("my_parquet").get().get(Constants.FORMAT()).contains(Constants.PARQUET()));
        assertTrue(result.connectionConfigByName().get("my_parquet").get().get(Constants.PATH()).contains("/path/parquet"));
        assertTrue(result.connectionConfigByName().contains("my_orc"));
        assertTrue(result.connectionConfigByName().get("my_orc").get().get(Constants.FORMAT()).contains(Constants.ORC()));
        assertTrue(result.connectionConfigByName().get("my_orc").get().get(Constants.PATH()).contains("/path/orc"));
        assertTrue(result.connectionConfigByName().contains("my_json"));
        assertTrue(result.connectionConfigByName().get("my_json").get().get(Constants.FORMAT()).contains(Constants.JSON()));
        assertTrue(result.connectionConfigByName().get("my_json").get().get(Constants.PATH()).contains("/path/json"));

        assertTrue(result.connectionConfigByName().contains("my_postgres"));
        var postgres = result.connectionConfigByName().get("my_postgres").get();
        assertTrue(postgres.get(Constants.FORMAT()).contains(Constants.JDBC()));
        assertTrue(postgres.get(Constants.URL()).contains("jdbc:postgresql://localhost:5432"));
        assertTrue(postgres.get(Constants.USERNAME()).contains("postgres"));
        assertTrue(postgres.get(Constants.PASSWORD()).contains("postgres"));
        assertTrue(postgres.get(Constants.DRIVER()).contains(Constants.POSTGRES_DRIVER()));

        assertTrue(result.connectionConfigByName().contains("my_mysql"));
        var mysql = result.connectionConfigByName().get("my_mysql").get();
        assertTrue(mysql.get(Constants.FORMAT()).contains(Constants.JDBC()));
        assertTrue(mysql.get(Constants.URL()).contains("jdbc:mysql://localhost:3306"));
        assertTrue(mysql.get(Constants.USERNAME()).contains("root"));
        assertTrue(mysql.get(Constants.PASSWORD()).contains("root"));
        assertTrue(mysql.get(Constants.DRIVER()).contains(Constants.MYSQL_DRIVER()));

        assertTrue(result.connectionConfigByName().contains("my_cassandra"));
        var cassandra = result.connectionConfigByName().get("my_cassandra").get();
        assertTrue(cassandra.get(Constants.FORMAT()).contains(Constants.CASSANDRA()));
        assertTrue(cassandra.get("spark.cassandra.connection.host").contains("localhost"));
        assertTrue(cassandra.get("spark.cassandra.connection.port").contains("9042"));
        assertTrue(cassandra.get("spark.cassandra.auth.username").contains("cassandra"));
        assertTrue(cassandra.get("spark.cassandra.auth.password").contains("cassandra"));

        assertTrue(result.connectionConfigByName().contains("my_jms"));
        assertTrue(result.connectionConfigByName().get("my_jms").get().get(Constants.FORMAT()).contains(Constants.JMS()));
        assertTrue(result.connectionConfigByName().contains("my_solace"));
        var solace = result.connectionConfigByName().get("my_solace").get();
        assertTrue(solace.get(Constants.FORMAT()).contains(Constants.JMS()));
        assertTrue(solace.get(Constants.URL()).contains("smf://localhost:55554"));
        assertTrue(solace.get(Constants.USERNAME()).contains("admin"));
        assertTrue(solace.get(Constants.PASSWORD()).contains("admin"));
        assertTrue(solace.get(Constants.JMS_VPN_NAME()).contains("default"));
        assertTrue(solace.get(Constants.JMS_CONNECTION_FACTORY()).contains(Constants.DEFAULT_SOLACE_CONNECTION_FACTORY()));
        assertTrue(solace.get(Constants.JMS_INITIAL_CONTEXT_FACTORY()).contains(Constants.DEFAULT_SOLACE_INITIAL_CONTEXT_FACTORY()));

        assertTrue(result.connectionConfigByName().contains("my_kafka"));
        assertTrue(result.connectionConfigByName().get("my_kafka").get().get(Constants.FORMAT()).contains(Constants.KAFKA()));
        assertTrue(result.connectionConfigByName().get("my_kafka").get().get("kafka.bootstrap.servers").contains("localhost:9092"));

        assertTrue(result.connectionConfigByName().contains("my_http"));
        assertTrue(result.connectionConfigByName().get("my_http").get().get(Constants.FORMAT()).contains(Constants.HTTP()));
    }

    @Test
    public void canCreateDataCatererConfigurationWithFlags() {
        var result = new DataCatererConfigurationBuilder()
                .enableCount(false)
                .enableGenerateData(false)
                .enableDeleteGeneratedRecords(true)
                .enableFailOnError(false)
                .enableGeneratePlanAndTasks(true)
                .enableRecordTracking(true)
                .enableSaveReports(false)
                .enableSinkMetadata(true)
                .enableUniqueCheck(true)
                .enableValidation(true)
                .configuration().build();

        assertFalse(result.flagsConfig().enableCount());
        assertFalse(result.flagsConfig().enableGenerateData());
        assertTrue(result.flagsConfig().enableDeleteGeneratedRecords());
        assertFalse(result.flagsConfig().enableFailOnError());
        assertTrue(result.flagsConfig().enableGeneratePlanAndTasks());
        assertTrue(result.flagsConfig().enableRecordTracking());
        assertFalse(result.flagsConfig().enableSaveReports());
        assertTrue(result.flagsConfig().enableSinkMetadata());
        assertTrue(result.flagsConfig().enableUniqueCheck());
        assertTrue(result.flagsConfig().enableValidation());
    }

    @Test
    public void canCreateDataCatererConfigurationWithFolders() {
        var result = new DataCatererConfigurationBuilder()
                .planFilePath("/my/plan")
                .taskFolderPath("/my/task")
                .recordTrackingFolderPath("/my/record")
                .validationFolderPath("/my/validation")
                .generatedReportsFolderPath("/my/report")
                .generatedPlanAndTaskFolderPath("/my/gen")
                .configuration().build();

        assertEquals("/my/plan", result.foldersConfig().planFilePath());
        assertEquals("/my/task", result.foldersConfig().taskFolderPath());
        assertEquals("/my/record", result.foldersConfig().recordTrackingFolderPath());
        assertEquals("/my/validation", result.foldersConfig().validationFolderPath());
        assertEquals("/my/report", result.foldersConfig().generatedReportsFolderPath());
        assertEquals("/my/gen", result.foldersConfig().generatedPlanAndTaskFolderPath());
    }

    @Test
    public void canCreateDataCatererConfigurationWithMetadata() {
        var result = new DataCatererConfigurationBuilder()
                .numRecordsFromDataSourceForDataProfiling(10)
                .numRecordsForAnalysisForDataProfiling(10)
                .numGeneratedSamples(1)
                .oneOfMinCount(10)
                .oneOfDistinctCountVsCountThreshold(0.5)
                .configuration().build();

        assertEquals(10, result.metadataConfig().numRecordsFromDataSource());
        assertEquals(10, result.metadataConfig().numRecordsForAnalysis());
        assertEquals(1, result.metadataConfig().numGeneratedSamples());
        assertEquals(10, result.metadataConfig().oneOfMinCount());
        assertEquals(0.5, result.metadataConfig().oneOfDistinctCountVsCountThreshold(), 0.01);
    }

    @Test
    public void canCreateDataCatererConfigurationWithGeneration() {
        var result = new DataCatererConfigurationBuilder()
                .numRecordsPerBatch(10)
                .numRecordsPerStep(10)
                .configuration().build();

        assertEquals(10, result.generationConfig().numRecordsPerBatch());
        assertTrue(result.generationConfig().numRecordsPerStep().contains(10));
    }
}