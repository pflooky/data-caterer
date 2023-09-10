package com.github.pflooky.datacaterer.api.java.model.config;

import com.github.pflooky.datacaterer.api.model.Constants;
import com.github.pflooky.datacaterer.api.model.DataCatererConfiguration;
import com.github.pflooky.datacaterer.api.model.FlagsConfig;
import com.github.pflooky.datacaterer.api.model.FoldersConfig;
import com.github.pflooky.datacaterer.api.model.GenerationConfig;
import com.github.pflooky.datacaterer.api.model.MetadataConfig;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaMap;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaOption;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaTuple;

public final class DataCatererConfigurationBuilder {

    private final com.github.pflooky.datacaterer.api.DataCatererConfigurationBuilder scalaDef;

    public DataCatererConfigurationBuilder(com.github.pflooky.datacaterer.api.DataCatererConfigurationBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    public com.github.pflooky.datacaterer.api.DataCatererConfigurationBuilder configuration() {
        return scalaDef;
    }

    public DataCatererConfigurationBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.DataCatererConfigurationBuilder(
                new DataCatererConfiguration(
                        new FlagsConfig(
                                Constants.DEFAULT_ENABLE_COUNT(),
                                Constants.DEFAULT_ENABLE_GENERATE_DATA(),
                                Constants.DEFAULT_ENABLE_RECORD_TRACKING(),
                                Constants.DEFAULT_ENABLE_DELETE_GENERATED_RECORDS(),
                                Constants.DEFAULT_ENABLE_GENERATE_PLAN_AND_TASKS(),
                                Constants.DEFAULT_ENABLE_FAIL_ON_ERROR(),
                                Constants.DEFAULT_ENABLE_UNIQUE_CHECK(),
                                Constants.DEFAULT_ENABLE_SINK_METADATA(),
                                Constants.DEFAULT_ENABLE_SAVE_REPORTS(),
                                Constants.DEFAULT_ENABLE_VALIDATION()
                        ),
                        new FoldersConfig(
                                Constants.DEFAULT_PLAN_FILE_PATH(),
                                Constants.DEFAULT_TASK_FOLDER_PATH(),
                                Constants.DEFAULT_GENERATED_PLAN_AND_TASK_FOLDER_PATH(),
                                Constants.DEFAULT_GENERATED_REPORTS_FOLDER_PATH(),
                                Constants.DEFAULT_RECORD_TRACKING_FOLDER_PATH(),
                                Constants.DEFAULT_VALIDATION_FOLDER_PATH()
                        ),
                        new MetadataConfig(
                                Constants.DEFAULT_NUM_RECORD_FROM_DATA_SOURCE(),
                                Constants.DEFAULT_NUM_RECORD_FOR_ANALYSIS(),
                                Constants.DEFAULT_ONE_OF_DISTINCT_COUNT_VS_COUNT_THRESHOLD(),
                                Constants.DEFAULT_ONE_OF_MIN_COUNT(),
                                Constants.DEFAULT_NUM_GENERATED_SAMPLES()
                        ),
                        new GenerationConfig(
                                Constants.DEFAULT_NUM_RECORDS_PER_BATCH(),
                                toScalaOption(Optional.empty())
                        ),
                        toScalaMap(Collections.emptyMap()),
                        toScalaMap(Collections.emptyMap()),
                        Constants.DEFAULT_SPARK_MASTER()
                )
        );
    }

    public DataCatererConfigurationBuilder sparkMaster(String master) {
        return new DataCatererConfigurationBuilder(scalaDef.sparkMaster(master));
    }

    public DataCatererConfigurationBuilder sparkConfig(Map<String, String> config) {
        return new DataCatererConfigurationBuilder(scalaDef.sparkConfig(toScalaMap(config)));
    }

    public DataCatererConfigurationBuilder addSparkConfig(String key, String value) {
        return new DataCatererConfigurationBuilder(scalaDef.addSparkConfig(toScalaTuple(key, value)));
    }

    private DataCatererConfigurationBuilder connectionConfig(Map<String, Map<String, String>> connectionConfigByName) {
        var mappedConfigs = connectionConfigByName.entrySet().stream()
                .map(c -> Map.entry(c.getKey(), toScalaMap(c.getValue())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new DataCatererConfigurationBuilder(scalaDef.connectionConfig(toScalaMap(mappedConfigs)));
    }

    private DataCatererConfigurationBuilder addConnectionConfig(String name, String format, Map<String, String> connectionConfig) {
        return new DataCatererConfigurationBuilder(scalaDef.addConnectionConfig(name, format, toScalaMap(connectionConfig)));
    }

    private DataCatererConfigurationBuilder addConnectionConfig(String name, String format, String path, Map<String, String> connectionConfig) {
        return new DataCatererConfigurationBuilder(scalaDef.addConnectionConfig(name, format, path, toScalaMap(connectionConfig)));
    }

    public DataCatererConfigurationBuilder csv(String name, String path, Map<String, String> options) {
        return new DataCatererConfigurationBuilder(scalaDef.csv(name, path, toScalaMap(options)));
    }

    public DataCatererConfigurationBuilder csv(String name, String path) {
        return csv(name, path, Collections.emptyMap());
    }

    public DataCatererConfigurationBuilder parquet(String name, String path, Map<String, String> options) {
        return new DataCatererConfigurationBuilder(scalaDef.parquet(name, path, toScalaMap(options)));
    }

    public DataCatererConfigurationBuilder parquet(String name, String path) {
        return parquet(name, path, Collections.emptyMap());
    }

    public DataCatererConfigurationBuilder orc(String name, String path, Map<String, String> options) {
        return new DataCatererConfigurationBuilder(scalaDef.orc(name, path, toScalaMap(options)));
    }

    public DataCatererConfigurationBuilder orc(String name, String path) {
        return orc(name, path, Collections.emptyMap());
    }

    public DataCatererConfigurationBuilder json(String name, String path, Map<String, String> options) {
        return new DataCatererConfigurationBuilder(scalaDef.json(name, path, toScalaMap(options)));
    }

    public DataCatererConfigurationBuilder json(String name, String path) {
        return json(name, path, Collections.emptyMap());
    }

    public DataCatererConfigurationBuilder postgres(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return new DataCatererConfigurationBuilder(scalaDef.postgres(name, url, username, password, toScalaMap(options)));
    }

    public DataCatererConfigurationBuilder postgres(
            String name,
            String url,
            String username,
            String password
    ) {
        return postgres(name, url, username, password, Collections.emptyMap());
    }

    public DataCatererConfigurationBuilder postgres(
            String name,
            String url
    ) {
        return postgres(name, url, "postgres", "postgres");
    }

    public DataCatererConfigurationBuilder mysql(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return new DataCatererConfigurationBuilder(scalaDef.mysql(name, url, username, password, toScalaMap(options)));
    }

    public DataCatererConfigurationBuilder mysql(
            String name,
            String url,
            String username,
            String password
    ) {
        return mysql(name, url, username, password, Collections.emptyMap());
    }

    public DataCatererConfigurationBuilder mysql(String name, String url) {
        return mysql(name, url, "root", "root");
    }

    public DataCatererConfigurationBuilder cassandra(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return new DataCatererConfigurationBuilder(scalaDef.cassandra(name, url, username, password, toScalaMap(options)));
    }

    public DataCatererConfigurationBuilder cassandra(
            String name,
            String url,
            String username,
            String password
    ) {
        return cassandra(name, url, username, password, Collections.emptyMap());
    }

    public DataCatererConfigurationBuilder cassandra(String name, String url) {
        return cassandra(name, url, "cassandra", "cassandra");
    }

    public DataCatererConfigurationBuilder jms(String name, String url, String username, String password, Map<String, String> options) {
        return new DataCatererConfigurationBuilder(scalaDef.jms(name, url, username, password, toScalaMap(options)));
    }

    public DataCatererConfigurationBuilder solace(
            String name,
            String url,
            String username,
            String password,
            String vpnName,
            String connectionFactory,
            String initialContextFactory,
            Map<String, String> options
    ) {
        return new DataCatererConfigurationBuilder(scalaDef.solace(
                name,
                url,
                username,
                password,
                vpnName,
                connectionFactory,
                initialContextFactory,
                toScalaMap(options))
        );
    }

    public DataCatererConfigurationBuilder solace(
            String name,
            String url,
            String username,
            String password,
            String vpnName
    ) {
        return solace(
                name,
                url,
                username,
                password,
                vpnName,
                Constants.DEFAULT_SOLACE_CONNECTION_FACTORY(),
                Constants.DEFAULT_SOLACE_INITIAL_CONTEXT_FACTORY(),
                Collections.emptyMap()
        );
    }

    public DataCatererConfigurationBuilder solace(String name, String url) {
        return solace(
                name,
                url,
                "admin",
                "admin",
                "default"
        );
    }

    public DataCatererConfigurationBuilder kafka(String name, String url, Map<String, String> options) {
        return new DataCatererConfigurationBuilder(scalaDef.kafka(name, url, toScalaMap(options)));
    }

    public DataCatererConfigurationBuilder kafka(String name, String url) {
        return kafka(name, url, Collections.emptyMap());
    }

    public DataCatererConfigurationBuilder http(
            String name,
            String username,
            String password,
            Map<String, String> options
    ) {
        return new DataCatererConfigurationBuilder(scalaDef.http(name, username, password, toScalaMap(options)));
    }

    public DataCatererConfigurationBuilder http(String name) {
        return http(name, "", "", Collections.emptyMap());
    }


    public DataCatererConfigurationBuilder enableGenerateData(boolean enable) {
        return new DataCatererConfigurationBuilder(scalaDef.enableGenerateData(enable));
    }

    public DataCatererConfigurationBuilder enableCount(boolean enable) {
        return new DataCatererConfigurationBuilder(scalaDef.enableCount(enable));
    }

    public DataCatererConfigurationBuilder enableValidation(boolean enable) {
        return new DataCatererConfigurationBuilder(scalaDef.enableValidation(enable));
    }

    public DataCatererConfigurationBuilder enableFailOnError(boolean enable) {
        return new DataCatererConfigurationBuilder(scalaDef.enableFailOnError(enable));
    }

    public DataCatererConfigurationBuilder enableUniqueCheck(boolean enable) {
        return new DataCatererConfigurationBuilder(scalaDef.enableUniqueCheck(enable));
    }

    public DataCatererConfigurationBuilder enableSaveReports(boolean enable) {
        return new DataCatererConfigurationBuilder(scalaDef.enableSaveReports(enable));
    }

    public DataCatererConfigurationBuilder enableSinkMetadata(boolean enable) {
        return new DataCatererConfigurationBuilder(scalaDef.enableSinkMetadata(enable));
    }

    public DataCatererConfigurationBuilder enableDeleteGeneratedRecords(boolean enable) {
        return new DataCatererConfigurationBuilder(scalaDef.enableDeleteGeneratedRecords(enable));
    }

    public DataCatererConfigurationBuilder enableGeneratePlanAndTasks(boolean enable) {
        return new DataCatererConfigurationBuilder(scalaDef.enableGeneratePlanAndTasks(enable));
    }

    public DataCatererConfigurationBuilder enableRecordTracking(boolean enable) {
        return new DataCatererConfigurationBuilder(scalaDef.enableRecordTracking(enable));
    }


    public DataCatererConfigurationBuilder planFilePath(String path) {
        return new DataCatererConfigurationBuilder(scalaDef.planFilePath(path));
    }

    public DataCatererConfigurationBuilder taskFolderPath(String path) {
        return new DataCatererConfigurationBuilder(scalaDef.taskFolderPath(path));
    }

    public DataCatererConfigurationBuilder recordTrackingFolderPath(String path) {
        return new DataCatererConfigurationBuilder(scalaDef.recordTrackingFolderPath(path));
    }

    public DataCatererConfigurationBuilder validationFolderPath(String path) {
        return new DataCatererConfigurationBuilder(scalaDef.validationFolderPath(path));
    }

    public DataCatererConfigurationBuilder generatedReportsFolderPath(String path) {
        return new DataCatererConfigurationBuilder(scalaDef.generatedReportsFolderPath(path));
    }

    public DataCatererConfigurationBuilder generatedPlanAndTaskFolderPath(String path) {
        return new DataCatererConfigurationBuilder(scalaDef.generatedPlanAndTaskFolderPath(path));
    }


    public DataCatererConfigurationBuilder numRecordsFromDataSourceForDataProfiling(int numRecords) {
        return new DataCatererConfigurationBuilder(scalaDef.numRecordsFromDataSourceForDataProfiling(numRecords));
    }

    public DataCatererConfigurationBuilder numRecordsForAnalysisForDataProfiling(int numRecords) {
        return new DataCatererConfigurationBuilder(scalaDef.numRecordsForAnalysisForDataProfiling(numRecords));
    }

    public DataCatererConfigurationBuilder numGeneratedSamples(int numSamples) {
        return new DataCatererConfigurationBuilder(scalaDef.numGeneratedSamples(numSamples));
    }

    public DataCatererConfigurationBuilder oneOfMinCount(long minCount) {
        return new DataCatererConfigurationBuilder(scalaDef.oneOfMinCount(minCount));
    }

    public DataCatererConfigurationBuilder oneOfDistinctCountVsCountThreshold(double threshold) {
        return new DataCatererConfigurationBuilder(scalaDef.oneOfDistinctCountVsCountThreshold(threshold));
    }


    public DataCatererConfigurationBuilder numRecordsPerBatch(long numRecords) {
        return new DataCatererConfigurationBuilder(scalaDef.numRecordsPerBatch(numRecords));
    }

    public DataCatererConfigurationBuilder numRecordsPerStep(long numRecords) {
        return new DataCatererConfigurationBuilder(scalaDef.numRecordsPerStep(numRecords));
    }
}
