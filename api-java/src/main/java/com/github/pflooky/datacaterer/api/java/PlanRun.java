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
import com.github.pflooky.datacaterer.api.java.model.config.connection.KafkaBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.connection.MySqlBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.connection.PostgresBuilder;
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

    /**
     * Create new CSV generation step with configurations
     * @param name Data source name
     * @param path File path to generated CSV
     * @param options Additional options for CSV generation
     * @return FileBuilder
     */
    public FileBuilder csv(String name, String path, Map<String, String> options) {
        return new ConnectionConfigWithTaskBuilder().file(name, Constants.CSV(), path, options);
    }

    /**
     * Create new CSV generation step with path
     * @param name Data source name
     * @param path File path to generated CSV
     * @return FileBuilder
     */
    public FileBuilder csv(String name, String path) {
        return csv(name, path, Collections.emptyMap());
    }

    /**
     * Create new CSV generation step based off existing file generation configuration
     * @param connectionTaskBuilder Existing file generation configuration
     * @return FileBuilder
     */
    public FileBuilder csv(ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.FileBuilder, FileBuilder> connectionTaskBuilder) {
        return new FileBuilder(connectionTaskBuilder.connectionTaskBuilder());
    }

    /**
     * Create new JSON generation step with configurations
     * @param name Data source name
     * @param path File path to generated JSON
     * @param options Additional options for JSON generation
     * @return FileBuilder
     */
    public FileBuilder json(String name, String path, Map<String, String> options) {
        return new ConnectionConfigWithTaskBuilder().file(name, Constants.JSON(), path, options);
    }

    /**
     * Create new JSON generation step with path
     * @param name Data source name
     * @param path File path to generated JSON
     * @return FileBuilder
     */
    public FileBuilder json(String name, String path) {
        return json(name, path, Collections.emptyMap());
    }

    /**
     * Create new JSON generation step based off existing file generation configuration
     * @param connectionTaskBuilder Existing file generation configuration
     * @return FileBuilder
     */
    public FileBuilder json(ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.FileBuilder, FileBuilder> connectionTaskBuilder) {
        return new FileBuilder(connectionTaskBuilder.connectionTaskBuilder());
    }

    /**
     * Create new ORC generation step with configurations
     * @param name Data source name
     * @param path File path to generated ORC
     * @param options Additional options for ORC generation
     * @return FileBuilder
     */
    public FileBuilder orc(String name, String path, Map<String, String> options) {
        return new ConnectionConfigWithTaskBuilder().file(name, Constants.ORC(), path, options);
    }

    /**
     * Create new ORC generation step with path
     * @param name Data source name
     * @param path File path to generated ORC
     * @return FileBuilder
     */
    public FileBuilder orc(String name, String path) {
        return orc(name, path, Collections.emptyMap());
    }

    /**
     * Create new ORC generation step based off existing file generation configuration
     * @param connectionTaskBuilder Existing file generation configuration
     * @return FileBuilder
     */
    public FileBuilder orc(ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.FileBuilder, FileBuilder> connectionTaskBuilder) {
        return new FileBuilder(connectionTaskBuilder.connectionTaskBuilder());
    }

    /**
     * Create new PARQUET generation step with configurations
     * @param name Data source name
     * @param path File path to generated PARQUET
     * @param options Additional options for PARQUET generation
     * @return FileBuilder
     */
    public FileBuilder parquet(String name, String path, Map<String, String> options) {
        return new ConnectionConfigWithTaskBuilder().file(name, Constants.PARQUET(), path, options);
    }

    /**
     * Create new PARQUET generation step with path
     * @param name Data source name
     * @param path File path to generated PARQUET
     * @return FileBuilder
     */
    public FileBuilder parquet(String name, String path) {
        return parquet(name, path, Collections.emptyMap());
    }

    /**
     * Create new PARQUET generation step based off existing file generation configuration
     * @param connectionTaskBuilder Existing file generation configuration
     * @return FileBuilder
     */
    public FileBuilder parquet(ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.FileBuilder, FileBuilder> connectionTaskBuilder) {
        return new FileBuilder(connectionTaskBuilder.connectionTaskBuilder());
    }

    /**
     * Create new POSTGRES generation step with connection configuration
     * @param name Data source name
     * @param url Postgres url in format: jdbc:postgresql://_host_:_port_/_database_
     * @param username Postgres username
     * @param password Postgres password
     * @param options Additional driver options
     * @return PostgresBuilder
     */
    public PostgresBuilder postgres(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return new ConnectionConfigWithTaskBuilder().postgres(name, url, username, password, options);
    }

    /**
     * Create new POSTGRES generation step with only Postgres URL and default username and password of 'postgres'
     * @param name Data source name
     * @param url Postgres url in format: jdbc:postgresql://_host_:_port_/_database_
     * @return PostgresBuilder
     */
    public PostgresBuilder postgres(String name, String url) {
        return postgres(
                name,
                url,
                Constants.DEFAULT_POSTGRES_USERNAME(),
                Constants.DEFAULT_POSTGRES_PASSWORD(),
                Collections.emptyMap()
        );
    }

    /**
     * Create new POSTGRES generation step using the same connection configuration from another PostgresBuilder
     * @param connectionTaskBuilder Postgres builder with connection configuration
     * @return PostgresBuilder
     */
    public PostgresBuilder postgres(ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.PostgresBuilder, PostgresBuilder> connectionTaskBuilder) {
        return new PostgresBuilder(connectionTaskBuilder.connectionTaskBuilder());
    }

    /**
     * Create new MYSQL generation step with connection configuration
     * @param name Data source name
     * @param url Mysql url in format: jdbc:mysql://_host_:_port_/_database_
     * @param username Mysql username
     * @param password Mysql password
     * @param options Additional driver options
     * @return MySqlBuilder
     */
    public MySqlBuilder mysql(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return new ConnectionConfigWithTaskBuilder().mysql(name, url, username, password, options);
    }

    /**
     * Create new MYSQL generation step with only Mysql URL and default username and password of 'root'
     * @param name Data source name
     * @param url Mysql url in format: jdbc:mysql://_host_:_port_/_dbname_
     * @return MySqlBuilder
     */
    public MySqlBuilder mysql(String name, String url) {
        return mysql(
                name,
                url,
                Constants.DEFAULT_MYSQL_USERNAME(),
                Constants.DEFAULT_MYSQL_PASSWORD(),
                Collections.emptyMap()
        );
    }

    /**
     * Create new MYSQL generation step using the same connection configuration from another MySqlBuilder
     * @param connectionTaskBuilder Mysql builder with connection configuration
     * @return MySqlBuilder
     */
    public MySqlBuilder mysql(ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.MySqlBuilder, MySqlBuilder> connectionTaskBuilder) {
        return new MySqlBuilder(connectionTaskBuilder.connectionTaskBuilder());
    }

    /**
     * Create new CASSANDRA generation step with connection configuration
     * @param name Data source name
     * @param url Cassandra url with format: _host_:_port_
     * @param username Cassandra username
     * @param password Cassandra password
     * @param options Additional connection options
     * @return CassandraBuilder
     */
    public CassandraBuilder cassandra(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return new ConnectionConfigWithTaskBuilder().cassandra(name, url, username, password, options);
    }

    /**
     * Create new CASSANDRA generation step with only Cassandra URL and default username and password of 'cassandra'
     * @param name Data source name
     * @param url Cassandra url with format: _host_:_port_
     * @return CassandraBuilder
     */
    public CassandraBuilder cassandra(String name, String url) {
        return cassandra(
                name,
                url,
                Constants.DEFAULT_CASSANDRA_USERNAME(),
                Constants.DEFAULT_CASSANDRA_PASSWORD(),
                Collections.emptyMap()
        );
    }

    /**
     * Create new Cassandra generation step using the same connection configuration from another CassandraBuilder
     * @param connectionTaskBuilder Cassandra builder with connection configuration
     * @return CassandraBuilder
     */
    public CassandraBuilder cassandra(ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.CassandraBuilder, CassandraBuilder> connectionTaskBuilder) {
        return new CassandraBuilder(connectionTaskBuilder.connectionTaskBuilder());
    }

    /**
     * Create new SOLACE generation step with connection configuration
     * @param name Data source name
     * @param url Solace url
     * @param username Solace username
     * @param password Solace password
     * @param vpnName VPN name in Solace to connect to
     * @param connectionFactory Connection factory
     * @param initialContextFactory Initial context factory
     * @param options Additional connection options
     * @return SolaceBuilder
     */
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

    /**
     * Create new SOLACE generation step with Solace URL, username, password and vpnName. Default connection factory and
     * initial context factory used
     * @param name Data source name
     * @param url Solace url
     * @param username Solace username
     * @param password Solace password
     * @param vpnName VPN name in Solace to connect to
     * @return SolaceBuilder
     */
    public SolaceBuilder solace(
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

    /**
     * Create new SOLACE generation step with Solace URL. Other configurations are set to default values
     * @param name Data source name
     * @param url Solace url
     * @return SolaceBuilder
     */
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

    /**
     * Create new Solace generation step using the same connection configuration from another SolaceBuilder
     * @param connectionTaskBuilder Solace step with connection configuration
     * @return SolaceBuilder
     */
    public SolaceBuilder solace(ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.SolaceBuilder, SolaceBuilder> connectionTaskBuilder) {
        return new SolaceBuilder(connectionTaskBuilder.connectionTaskBuilder());
    }

    /**
     * Create new KAFKA generation step with connection configuration
     * @param name Data source name
     * @param url Kafka url
     * @param options Additional connection options
     * @return KafkaBuilder
     */
    public KafkaBuilder kafka(String name, String url, Map<String, String> options) {
        return new ConnectionConfigWithTaskBuilder().kafka(name, url, options);
    }

    /**
     * Create new KAFKA generation step with url
     * @param name Data source name
     * @param url Kafka url
     * @return KafkaBuilder
     */
    public KafkaBuilder kafka(String name, String url) {
        return kafka(name, url, Collections.emptyMap());
    }

    /**
     * Create new Kafka generation step using the same connection configuration from another KafkaBuilder
     * @param connectionTaskBuilder Kafka step with connection configuration
     * @return KafkaBuilder
     */
    public KafkaBuilder kafka(ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.KafkaBuilder, KafkaBuilder> connectionTaskBuilder) {
        return new KafkaBuilder(connectionTaskBuilder.connectionTaskBuilder());
    }

    /**
     * Create new HTTP generation step using connection configuration
     * @param name Data source name
     * @param username HTTP username
     * @param password HTTP password
     * @param options Additional connection options
     * @return HttpBuilder
     */
    public HttpBuilder http(String name, String username, String password, Map<String, String> options) {
        return new ConnectionConfigWithTaskBuilder().http(name, username, password, options);
    }

    /**
     * Create new HTTP generation step without authentication
     * @param name Data source name
     * @return HttpBuilder
     */
    public HttpBuilder http(String name) {
        return http(name, "", "", Collections.emptyMap());
    }

    /**
     * Create new HTTP generation step using the same connection configuration from another HttpBuilder
     * @param connectionTaskBuilder Http step with connection configuration
     * @return HttpBuilder
     */
    public HttpBuilder http(ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.HttpBuilder, HttpBuilder> connectionTaskBuilder) {
        return new HttpBuilder(connectionTaskBuilder.connectionTaskBuilder());
    }


    /**
     * Execute with the following connections and tasks defined
     * @param connectionTaskBuilder First connection and task
     * @param connectionTaskBuilders Other connections and tasks
     */
    public void execute(ConnectionTaskBuilder connectionTaskBuilder, ConnectionTaskBuilder... connectionTaskBuilders) {
        execute(configuration(), Collections.emptyList(), connectionTaskBuilder, connectionTaskBuilders);
    }

    /**
     * Execute with non-default configurations for a set of tasks
     * @param configurationBuilder Runtime configurations
     * @param connectionTaskBuilder First connection and task
     * @param connectionTaskBuilders Other connections and tasks
     */
    public void execute(
            DataCatererConfigurationBuilder configurationBuilder,
            ConnectionTaskBuilder connectionTaskBuilder,
            ConnectionTaskBuilder... connectionTaskBuilders
    ) {
        execute(configurationBuilder, Collections.emptyList(), connectionTaskBuilder, connectionTaskBuilders);
    }

    /**
     * Execute with non-default configurations with validations and tasks. Validations have to be enabled before running
     * (see {@link DataCatererConfigurationBuilder#enableValidation(boolean)}.
     * @param configurationBuilder Runtime configurations
     * @param validations Validations to run if enabled
     * @param connectionTaskBuilder First connection and task
     * @param connectionTaskBuilders Other connections and tasks
     */
    public void execute(
            DataCatererConfigurationBuilder configurationBuilder,
            List<ValidationConfigurationBuilder> validations,
            ConnectionTaskBuilder<?, ?> connectionTaskBuilder,
            ConnectionTaskBuilder<?, ?>... connectionTaskBuilders
    ) {
        var planWithConfig = new com.github.pflooky.datacaterer.api.BasePlanRun();
        List<com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<?>> listConnectionTaskBuilders =
                Arrays.stream(connectionTaskBuilders)
                        .map(ConnectionTaskBuilder::connectionTaskBuilder)
                        .collect(Collectors.toList());
        planWithConfig.execute(
                plan().plan(),
                configurationBuilder.configuration(),
                toScalaList(validations.stream().map(ValidationConfigurationBuilder::configuration).collect(Collectors.toList())),
                connectionTaskBuilder.connectionTaskBuilder(),
                toScalaSeq(listConnectionTaskBuilders)
        );
        this.basePlanRun = planWithConfig;
    }

    /**
     * Execute with set of tasks and default configurations
     * @param tasks Tasks to generate data
     */
    public void execute(TasksBuilder tasks) {
        execute(List.of(tasks), plan(), configuration(), Collections.emptyList());
    }

    /**
     * Execute with plan and non-default configuration
     * @param plan Plan to set high level task configurations
     * @param configuration Runtime configuration
     */
    public void execute(PlanBuilder plan, DataCatererConfigurationBuilder configuration) {
        execute(Collections.emptyList(), plan, configuration, Collections.emptyList());
    }

    /**
     * Execute with tasks, plan, runtime configurations and validations defined
     * @param tasks Set of generation tasks
     * @param plan Plan to set high level task configurations
     * @param configuration Runtime configurations
     * @param validations Validations on data sources
     */
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
