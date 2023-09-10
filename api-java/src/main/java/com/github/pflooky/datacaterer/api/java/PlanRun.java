package com.github.pflooky.datacaterer.api.java;


import com.github.pflooky.datacaterer.api.java.model.CountBuilder;
import com.github.pflooky.datacaterer.api.java.model.FieldBuilder;
import com.github.pflooky.datacaterer.api.java.model.GeneratorBuilder;
import com.github.pflooky.datacaterer.api.java.model.StepBuilder;
import com.github.pflooky.datacaterer.api.java.model.TaskBuilder;
import com.github.pflooky.datacaterer.api.java.model.TaskSummaryBuilder;
import com.github.pflooky.datacaterer.api.java.model.TasksBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.ConnectionConfigWithTaskBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.DataCatererConfigurationBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.connection.CassandraBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.connection.ConnectionTaskBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.connection.FileBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.connection.HttpBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.connection.JdbcBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.connection.KafkaBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.connection.SolaceBuilder;
import com.github.pflooky.datacaterer.api.java.model.validation.DataSourceValidationBuilder;
import com.github.pflooky.datacaterer.api.java.model.validation.ValidationBuilder;
import com.github.pflooky.datacaterer.api.java.model.validation.ValidationConfigurationBuilder;
import com.github.pflooky.datacaterer.api.model.Constants;
import com.github.pflooky.datacaterer.api.model.DataCatererConfiguration;
import com.github.pflooky.datacaterer.api.model.ForeignKeyRelation;
import com.github.pflooky.datacaterer.api.model.Task;
import com.github.pflooky.datacaterer.api.model.ValidationConfiguration;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaList;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaSeq;

public abstract class PlanRun {

    private com.github.pflooky.datacaterer.api.PlanRun basePlanRun;
    private PlanRun plan;
    private List<Task> tasks;
    private DataCatererConfiguration configuration;
    private List<ValidationConfiguration> validations;

    public com.github.pflooky.datacaterer.api.PlanRun getPlan() {
        return basePlanRun;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public DataCatererConfiguration getConfiguration() {
        return configuration;
    }

    public List<ValidationConfiguration> getValidations() {
        return validations;
    }

    public PlanBuilder plan() {
        return new PlanBuilder();
    }

    public TaskSummaryBuilder taskSummary() {
        return new TaskSummaryBuilder();
    }

    public TasksBuilder tasks() {
        return new TasksBuilder();
    }

    public TaskBuilder task() {
        return new TaskBuilder();
    }

    public StepBuilder step() {
        return new StepBuilder();
    }

    public SchemaBuilder schema() {
        return new SchemaBuilder();
    }

    public FieldBuilder field() {
        return new FieldBuilder();
    }

    public GeneratorBuilder generator() {
        return new GeneratorBuilder();
    }

    public CountBuilder count() {
        return new CountBuilder();
    }

    public DataCatererConfigurationBuilder configuration() {
        return new DataCatererConfigurationBuilder();
    }

    public ValidationBuilder validation() {
        return new ValidationBuilder();
    }

    public DataSourceValidationBuilder dataSourceValidation() {
        return new DataSourceValidationBuilder();
    }

    public ValidationConfigurationBuilder validationConfig() {
        return new ValidationConfigurationBuilder();
    }

    public ForeignKeyRelation foreignField(String dataSource, String step, String column) {
        return new ForeignKeyRelation(dataSource, step, column);
    }

    public FileBuilder csv(String name, String path, Map<String, String> options) {
        return new ConnectionConfigWithTaskBuilder().file(name, Constants.CSV(), path, options);
    }

    public FileBuilder csv(String name, String path) {
        return csv(name, path, Collections.emptyMap());
    }

    public FileBuilder json(String name, String path, Map<String, String> options) {
        return new ConnectionConfigWithTaskBuilder().file(name, Constants.JSON(), path, options);
    }

    public FileBuilder json(String name, String path) {
        return json(name, path, Collections.emptyMap());
    }

    public FileBuilder orc(String name, String path, Map<String, String> options) {
        return new ConnectionConfigWithTaskBuilder().file(name, Constants.ORC(), path, options);
    }

    public FileBuilder orc(String name, String path) {
        return orc(name, path, Collections.emptyMap());
    }

    public FileBuilder parquet(String name, String path, Map<String, String> options) {
        return new ConnectionConfigWithTaskBuilder().file(name, Constants.PARQUET(), path, options);
    }

    public FileBuilder parquet(String name, String path) {
        return parquet(name, path, Collections.emptyMap());
    }

    public JdbcBuilder postgres(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return new ConnectionConfigWithTaskBuilder().jdbc(name, Constants.POSTGRES(), url, username, password, options);
    }

    public JdbcBuilder postgres(String name, String url) {
        return postgres(
                name,
                url,
                Constants.DEFAULT_POSTGRES_USERNAME(),
                Constants.DEFAULT_POSTGRES_PASSWORD(),
                Collections.emptyMap()
        );
    }

    public JdbcBuilder mysql(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return new ConnectionConfigWithTaskBuilder().jdbc(name, Constants.MYSQL(), url, username, password, options);
    }

    public JdbcBuilder mysql(String name, String url) {
        return mysql(
                name,
                url,
                Constants.DEFAULT_MYSQL_USERNAME(),
                Constants.DEFAULT_MYSQL_PASSWORD(),
                Collections.emptyMap()
        );
    }

    public CassandraBuilder cassandra(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return new ConnectionConfigWithTaskBuilder().cassandra(name, url, username, password, options);
    }

    public CassandraBuilder cassandra(String name, String url) {
        return cassandra(
                name,
                url,
                Constants.DEFAULT_CASSANDRA_USERNAME(),
                Constants.DEFAULT_CASSANDRA_PASSWORD(),
                Collections.emptyMap()
        );
    }

    public SolaceBuilder solace(
            String name,
            String url,
            String username,
            String password,
            String vpnName,
            String connectionFactory,
            String initialContextFactory,
            Map<String, String> options
    ) {
        return new ConnectionConfigWithTaskBuilder().solace(name, url, username, password, vpnName, connectionFactory, initialContextFactory, options);
    }

    public SolaceBuilder solace(String name, String url) {
        return solace(
                name,
                url,
                Constants.DEFAULT_SOLACE_USERNAME(),
                Constants.DEFAULT_SOLACE_PASSWORD(),
                Constants.DEFAULT_SOLACE_VPN_NAME(),
                Constants.DEFAULT_SOLACE_CONNECTION_FACTORY(),
                Constants.DEFAULT_SOLACE_INITIAL_CONTEXT_FACTORY(),
                Collections.emptyMap()
        );
    }

    public KafkaBuilder kafka(String name, String url, Map<String, String> options) {
        return new ConnectionConfigWithTaskBuilder().kafka(name, url, options);
    }

    public KafkaBuilder kafka(String name, String url) {
        return kafka(name, url, Collections.emptyMap());
    }

    public HttpBuilder http(String name, String username, String password, Map<String, String> options) {
        return new ConnectionConfigWithTaskBuilder().http(name, username, password, options);
    }

    public HttpBuilder http(String name) {
        return http(name, "", "", Collections.emptyMap());
    }


    public void execute(ConnectionTaskBuilder connectionTaskBuilder, ConnectionTaskBuilder... connectionTaskBuilders) {
        execute(configuration(), connectionTaskBuilder, connectionTaskBuilders);
    }

    public void execute(
            DataCatererConfigurationBuilder configurationBuilder,
            ConnectionTaskBuilder connectionTaskBuilder,
            ConnectionTaskBuilder... connectionTaskBuilders
    ) {
        var planWithConfig = new com.github.pflooky.datacaterer.api.BasePlanRun();
        planWithConfig.execute(
                configurationBuilder.configuration(),
                connectionTaskBuilder.connectionTaskBuilder(),
                toScalaSeq(Arrays.stream(connectionTaskBuilders).map(ConnectionTaskBuilder::connectionTaskBuilder).collect(Collectors.toList()))
        );
        this.basePlanRun = planWithConfig;
    }

    public void execute(TasksBuilder tasks) {
        execute(List.of(tasks), plan(), configuration(), Collections.emptyList());
    }

    public void execute(PlanBuilder plan, DataCatererConfigurationBuilder configuration) {
        execute(Collections.emptyList(), plan, configuration, Collections.emptyList());
    }

    public void execute(
            List<TasksBuilder> tasks,
            PlanBuilder plan,
            DataCatererConfigurationBuilder configuration,
            List<ValidationConfigurationBuilder> validations
    ) {
        var planWithConfig = new com.github.pflooky.datacaterer.api.BasePlanRun();
        planWithConfig.execute(
                toScalaList(tasks.stream().map(TasksBuilder::tasks).collect(Collectors.toList())),
                plan.plan(),
                configuration.configuration(),
                toScalaList(validations.stream().map(ValidationConfigurationBuilder::configuration).collect(Collectors.toList()))
        );
        this.basePlanRun = planWithConfig;
    }

}
