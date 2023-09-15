package com.github.pflooky.datacaterer.api.java;


import com.github.pflooky.datacaterer.api.BasePlanRun;
import com.github.pflooky.datacaterer.api.CountBuilder;
import com.github.pflooky.datacaterer.api.DataSourceValidationBuilder;
import com.github.pflooky.datacaterer.api.FieldBuilder;
import com.github.pflooky.datacaterer.api.GeneratorBuilder;
import com.github.pflooky.datacaterer.api.PlanBuilder;
import com.github.pflooky.datacaterer.api.SchemaBuilder;
import com.github.pflooky.datacaterer.api.StepBuilder;
import com.github.pflooky.datacaterer.api.TaskBuilder;
import com.github.pflooky.datacaterer.api.TaskSummaryBuilder;
import com.github.pflooky.datacaterer.api.ValidationBuilder;
import com.github.pflooky.datacaterer.api.model.ForeignKeyRelation;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaList;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaMap;

public abstract class PlanRun {

    private com.github.pflooky.datacaterer.api.PlanRun basePlanRun = new BasePlanRun();

    public com.github.pflooky.datacaterer.api.PlanRun getPlan() {
        return basePlanRun;
    }

    public com.github.pflooky.datacaterer.api.PlanBuilder plan() {
        return new com.github.pflooky.datacaterer.api.PlanBuilder();
    }

    public com.github.pflooky.datacaterer.api.TaskSummaryBuilder taskSummary() {
        return new TaskSummaryBuilder();
    }

    public com.github.pflooky.datacaterer.api.TasksBuilder tasks() {
        return new com.github.pflooky.datacaterer.api.TasksBuilder();
    }

    public com.github.pflooky.datacaterer.api.TaskBuilder task() {
        return new TaskBuilder();
    }

    public com.github.pflooky.datacaterer.api.StepBuilder step() {
        return new StepBuilder();
    }

    public com.github.pflooky.datacaterer.api.SchemaBuilder schema() {
        return new SchemaBuilder();
    }

    public com.github.pflooky.datacaterer.api.FieldBuilder field() {
        return new FieldBuilder();
    }

    public com.github.pflooky.datacaterer.api.GeneratorBuilder generator() {
        return new GeneratorBuilder();
    }

    public com.github.pflooky.datacaterer.api.CountBuilder count() {
        return new CountBuilder();
    }

    public com.github.pflooky.datacaterer.api.DataCatererConfigurationBuilder configuration() {
        return new com.github.pflooky.datacaterer.api.DataCatererConfigurationBuilder();
    }

    public com.github.pflooky.datacaterer.api.ValidationBuilder validation() {
        return new ValidationBuilder();
    }

    public com.github.pflooky.datacaterer.api.DataSourceValidationBuilder dataSourceValidation() {
        return new DataSourceValidationBuilder();
    }

    public com.github.pflooky.datacaterer.api.ValidationConfigurationBuilder validationConfig() {
        return new com.github.pflooky.datacaterer.api.ValidationConfigurationBuilder();
    }

    public ForeignKeyRelation foreignField(String dataSource, String step, String column) {
        return new ForeignKeyRelation(dataSource, step, column);
    }

    /**
     * Create new CSV generation step with configurations
     *
     * @param name    Data source name
     * @param path    File path to generated CSV
     * @param options Additional options for CSV generation
     * @return FileBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.FileBuilder csv(
            String name, String path, Map<String, String> options
    ) {
        return basePlanRun.csv(name, path, toScalaMap(options));
    }

    /**
     * Create new CSV generation step with path
     *
     * @param name Data source name
     * @param path File path to generated CSV
     * @return FileBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.FileBuilder csv(String name, String path) {
        return csv(name, path, Collections.emptyMap());
    }

    /**
     * Create new JSON generation step with configurations
     *
     * @param name    Data source name
     * @param path    File path to generated JSON
     * @param options Additional options for JSON generation
     * @return FileBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.FileBuilder json(String name, String path, Map<String, String> options) {
        return basePlanRun.json(name, path, toScalaMap(options));
    }

    /**
     * Create new JSON generation step with path
     *
     * @param name Data source name
     * @param path File path to generated JSON
     * @return FileBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.FileBuilder json(String name, String path) {
        return json(name, path, Collections.emptyMap());
    }

    /**
     * Create new ORC generation step with configurations
     *
     * @param name    Data source name
     * @param path    File path to generated ORC
     * @param options Additional options for ORC generation
     * @return FileBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.FileBuilder orc(String name, String path, Map<String, String> options) {
        return basePlanRun.orc(name, path, toScalaMap(options));
    }

    /**
     * Create new ORC generation step with path
     *
     * @param name Data source name
     * @param path File path to generated ORC
     * @return FileBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.FileBuilder orc(String name, String path) {
        return orc(name, path, Collections.emptyMap());
    }

    /**
     * Create new PARQUET generation step with configurations
     *
     * @param name    Data source name
     * @param path    File path to generated PARQUET
     * @param options Additional options for PARQUET generation
     * @return FileBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.FileBuilder parquet(String name, String path, Map<String, String> options) {
        return basePlanRun.parquet(name, path, toScalaMap(options));
    }

    /**
     * Create new PARQUET generation step with path
     *
     * @param name Data source name
     * @param path File path to generated PARQUET
     * @return FileBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.FileBuilder parquet(String name, String path) {
        return parquet(name, path, Collections.emptyMap());
    }

    /**
     * Create new POSTGRES generation step with connection configuration
     *
     * @param name     Data source name
     * @param url      Postgres url in format: jdbc:postgresql://_host_:_port_/_database_
     * @param username Postgres username
     * @param password Postgres password
     * @param options  Additional driver options
     * @return PostgresBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.PostgresBuilder postgres(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return basePlanRun.postgres(name, url, username, password, toScalaMap(options));
    }

    /**
     * Create new POSTGRES generation step with only Postgres URL and default username and password of 'postgres'
     *
     * @param name Data source name
     * @param url  Postgres url in format: jdbc:postgresql://_host_:_port_/_database_
     * @return PostgresBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.PostgresBuilder postgres(String name, String url) {
        return basePlanRun.postgresJava(name, url);
    }

    /**
     * Create new POSTGRES generation step using the same connection configuration from another PostgresBuilder
     *
     * @param connectionTaskBuilder Postgres builder with connection configuration
     * @return PostgresBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.PostgresBuilder postgres(
            com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.PostgresBuilder> connectionTaskBuilder
    ) {
        return basePlanRun.postgres(connectionTaskBuilder);
    }

    /**
     * Create new MYSQL generation step with connection configuration
     *
     * @param name     Data source name
     * @param url      Mysql url in format: jdbc:mysql://_host_:_port_/_database_
     * @param username Mysql username
     * @param password Mysql password
     * @param options  Additional driver options
     * @return MySqlBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.MySqlBuilder mysql(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return basePlanRun.mysql(name, url, username, password, toScalaMap(options));
    }

    /**
     * Create new MYSQL generation step with only Mysql URL and default username and password of 'root'
     *
     * @param name Data source name
     * @param url  Mysql url in format: jdbc:mysql://_host_:_port_/_dbname_
     * @return MySqlBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.MySqlBuilder mysql(String name, String url) {
        return basePlanRun.mysqlJava(name, url);
    }

    /**
     * Create new MYSQL generation step using the same connection configuration from another MySqlBuilder
     *
     * @param connectionTaskBuilder Mysql builder with connection configuration
     * @return MySqlBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.MySqlBuilder mysql(
            com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.MySqlBuilder> connectionTaskBuilder
    ) {
        return basePlanRun.mysql(connectionTaskBuilder);
    }

    /**
     * Create new CASSANDRA generation step with connection configuration
     *
     * @param name     Data source name
     * @param url      Cassandra url with format: _host_:_port_
     * @param username Cassandra username
     * @param password Cassandra password
     * @param options  Additional connection options
     * @return CassandraBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.CassandraBuilder cassandra(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return basePlanRun.cassandra(name, url, username, password, toScalaMap(options));
    }

    /**
     * Create new CASSANDRA generation step with only Cassandra URL and default username and password of 'cassandra'
     *
     * @param name Data source name
     * @param url  Cassandra url with format: _host_:_port_
     * @return CassandraBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.CassandraBuilder cassandra(String name, String url) {
        return basePlanRun.cassandraJava(name, url);
    }

    /**
     * Create new Cassandra generation step using the same connection configuration from another CassandraBuilder
     *
     * @param connectionTaskBuilder Cassandra builder with connection configuration
     * @return CassandraBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.CassandraBuilder cassandra(
            com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.CassandraBuilder> connectionTaskBuilder
    ) {
        return basePlanRun.cassandra(connectionTaskBuilder);
    }

    /**
     * Create new SOLACE generation step with connection configuration
     *
     * @param name                  Data source name
     * @param url                   Solace url
     * @param username              Solace username
     * @param password              Solace password
     * @param vpnName               VPN name in Solace to connect to
     * @param connectionFactory     Connection factory
     * @param initialContextFactory Initial context factory
     * @param options               Additional connection options
     * @return SolaceBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.SolaceBuilder solace(
            String name,
            String url,
            String username,
            String password,
            String vpnName,
            String connectionFactory,
            String initialContextFactory,
            Map<String, String> options
    ) {
        return basePlanRun.solace(name, url, username, password, vpnName, connectionFactory, initialContextFactory, toScalaMap(options));
    }

    /**
     * Create new SOLACE generation step with Solace URL, username, password and vpnName. Default connection factory and
     * initial context factory used
     *
     * @param name     Data source name
     * @param url      Solace url
     * @param username Solace username
     * @param password Solace password
     * @param vpnName  VPN name in Solace to connect to
     * @return SolaceBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.SolaceBuilder solace(
            String name,
            String url,
            String username,
            String password,
            String vpnName
    ) {
        return basePlanRun.solaceJava(name, url, username, password, vpnName);
    }

    /**
     * Create new SOLACE generation step with Solace URL. Other configurations are set to default values
     *
     * @param name Data source name
     * @param url  Solace url
     * @return SolaceBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.SolaceBuilder solace(String name, String url) {
        return basePlanRun.solaceJava(name, url);
    }

    /**
     * Create new Solace generation step using the same connection configuration from another SolaceBuilder
     *
     * @param connectionTaskBuilder Solace step with connection configuration
     * @return SolaceBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.SolaceBuilder solace(
            com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.SolaceBuilder> connectionTaskBuilder
    ) {
        return basePlanRun.solace(connectionTaskBuilder);
    }

    /**
     * Create new KAFKA generation step with connection configuration
     *
     * @param name    Data source name
     * @param url     Kafka url
     * @param options Additional connection options
     * @return KafkaBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.KafkaBuilder kafka(String name, String url, Map<String, String> options) {
        return basePlanRun.kafka(name, url, toScalaMap(options));
    }

    /**
     * Create new KAFKA generation step with url
     *
     * @param name Data source name
     * @param url  Kafka url
     * @return KafkaBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.KafkaBuilder kafka(String name, String url) {
        return basePlanRun.kafkaJava(name, url);
    }

    /**
     * Create new Kafka generation step using the same connection configuration from another KafkaBuilder
     *
     * @param connectionTaskBuilder Kafka step with connection configuration
     * @return KafkaBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.KafkaBuilder kafka(
            com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.KafkaBuilder> connectionTaskBuilder
    ) {
        return basePlanRun.kafka(connectionTaskBuilder);
    }

    /**
     * Create new HTTP generation step using connection configuration
     *
     * @param name     Data source name
     * @param username HTTP username
     * @param password HTTP password
     * @param options  Additional connection options
     * @return HttpBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.HttpBuilder http(String name, String username, String password, Map<String, String> options) {
        return basePlanRun.http(name, username, password, toScalaMap(options));
    }

    /**
     * Create new HTTP generation step without authentication
     *
     * @param name Data source name
     * @return HttpBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.HttpBuilder http(String name) {
        return basePlanRun.httpJava(name);
    }

    /**
     * Create new HTTP generation step using the same connection configuration from another HttpBuilder
     *
     * @param connectionTaskBuilder Http step with connection configuration
     * @return HttpBuilder
     */
    public com.github.pflooky.datacaterer.api.connection.HttpBuilder http(
            com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.HttpBuilder> connectionTaskBuilder
    ) {
        return basePlanRun.http(connectionTaskBuilder);
    }


    /**
     * Execute with the following connections and tasks defined
     *
     * @param connectionTaskBuilder  First connection and task
     * @param connectionTaskBuilders Other connections and tasks
     */
    public void execute(
            com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<?> connectionTaskBuilder,
            com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<?>... connectionTaskBuilders
    ) {
        execute(configuration(), Collections.emptyList(), connectionTaskBuilder, connectionTaskBuilders);
    }

    /**
     * Execute with non-default configurations for a set of tasks
     *
     * @param configurationBuilder   Runtime configurations
     * @param connectionTaskBuilder  First connection and task
     * @param connectionTaskBuilders Other connections and tasks
     */
    public void execute(
            com.github.pflooky.datacaterer.api.DataCatererConfigurationBuilder configurationBuilder,
            com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<?> connectionTaskBuilder,
            com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<?>... connectionTaskBuilders
    ) {
        execute(configurationBuilder, Collections.emptyList(), connectionTaskBuilder, connectionTaskBuilders);
    }

    /**
     * Execute with non-default configurations with validations and tasks. Validations have to be enabled before running
     * (see {@link DataCatererConfigurationBuilder#enableValidation(boolean)}).
     *
     * @param configurationBuilder   Runtime configurations
     * @param validations            Validations to run if enabled
     * @param connectionTaskBuilder  First connection and task
     * @param connectionTaskBuilders Other connections and tasks
     */
    public void execute(
            com.github.pflooky.datacaterer.api.DataCatererConfigurationBuilder configurationBuilder,
            List<com.github.pflooky.datacaterer.api.ValidationConfigurationBuilder> validations,
            com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<?> connectionTaskBuilder,
            com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<?>... connectionTaskBuilders
    ) {
        var planWithConfig = new com.github.pflooky.datacaterer.api.BasePlanRun();
        planWithConfig.execute(
                plan(),
                configurationBuilder,
                toScalaList(validations),
                connectionTaskBuilder,
                connectionTaskBuilders
        );
        this.basePlanRun = planWithConfig;
    }

    /**
     * Execute with set of tasks and default configurations
     *
     * @param tasks Tasks to generate data
     */
    public void execute(com.github.pflooky.datacaterer.api.TasksBuilder tasks) {
        execute(List.of(tasks), plan(), configuration(), Collections.emptyList());
    }

    /**
     * Execute with plan and non-default configuration
     *
     * @param plan          Plan to set high level task configurations
     * @param configuration Runtime configuration
     */
    public void execute(com.github.pflooky.datacaterer.api.PlanBuilder plan, com.github.pflooky.datacaterer.api.DataCatererConfigurationBuilder configuration) {
        execute(Collections.emptyList(), plan, configuration, Collections.emptyList());
    }

    /**
     * Execute with tasks, plan, runtime configurations and validations defined
     *
     * @param tasks         Set of generation tasks
     * @param plan          Plan to set high level task configurations
     * @param configuration Runtime configurations
     * @param validations   Validations on data sources
     */
    public void execute(
            List<com.github.pflooky.datacaterer.api.TasksBuilder> tasks,
            PlanBuilder plan,
            com.github.pflooky.datacaterer.api.DataCatererConfigurationBuilder configuration,
            List<com.github.pflooky.datacaterer.api.ValidationConfigurationBuilder> validations
    ) {
        var planWithConfig = new com.github.pflooky.datacaterer.api.BasePlanRun();
        planWithConfig.execute(
                toScalaList(tasks),
                plan,
                configuration,
                toScalaList(validations)
        );
        this.basePlanRun = planWithConfig;
    }

}
