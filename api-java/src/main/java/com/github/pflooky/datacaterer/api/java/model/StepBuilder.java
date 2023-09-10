package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.java.SchemaBuilder;
import com.github.pflooky.datacaterer.api.model.Constants;
import com.github.pflooky.datacaterer.api.model.Step;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaMap;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaSeq;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaTuple;

/**
 * Steps are a representation of a sub data source and is where you define various parameters that can control:<br>
 * 1. Number of records to generate<br>
 * 2. Schema<br>
 * 3. Sub data source details<br>
 */
public final class StepBuilder {
    private final com.github.pflooky.datacaterer.api.StepBuilder scalaDef;

    /**
     * Wrapper around Scala StepBuilder
     *
     * @param scalaDef Scala StepBuilder
     */
    public StepBuilder(com.github.pflooky.datacaterer.api.StepBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    /**
     * By default, a step will have the following defaults:<br>
     * name: generated unique name<br>
     * type: json<br>
     * count: 1000<br>
     * options: empty<br>
     * schema: empty<br>
     * enabled: true<br>
     */
    public StepBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.StepBuilder(
                new Step(
                        Constants.DEFAULT_STEP_NAME(),
                        Constants.DEFAULT_STEP_TYPE(),
                        new CountBuilder().count(),
                        toScalaMap(Collections.emptyMap()),
                        new SchemaBuilder().schema(),
                        Constants.DEFAULT_STEP_ENABLED()
                )
        );
    }

    /**
     * @return Scala definition of Step
     */
    public com.github.pflooky.datacaterer.api.model.Step step() {
        return scalaDef.step();
    }

    /**
     * Define name of step.
     * Used as part of foreign key definitions
     *
     * @param name Step name
     * @return StepBuilder
     */
    public StepBuilder name(String name) {
        return new StepBuilder(scalaDef.name(name));
    }

    /**
     * Define type of step.
     * Used to determine how to save the generated data
     *
     * @param type Can be one of the supported types
     * @return StepBuilder
     */
    public StepBuilder type(String type) {
        return new StepBuilder(scalaDef.type(type));
    }

    /**
     * Enable/disable the step
     *
     * @param enabled Boolean flag
     * @return StepBuilder
     */
    public StepBuilder enabled(boolean enabled) {
        return new StepBuilder(scalaDef.enabled(enabled));
    }

    /**
     * Add in generic option to the step.
     * This can be used to configure the sub data source details such as table, topic, and file path.
     * It is used as part of the options passed to Spark when connecting to the data source.
     * Can also be used for attaching metadata to the step
     *
     * @param key   Key of the data used for retrieval
     * @param value Value
     * @return StepBuilder
     */
    public StepBuilder option(String key, String value) {
        return new StepBuilder(scalaDef.option(toScalaTuple(key, value)));
    }

    /**
     * Map of configurations used by Spark to connect to the data source
     *
     * @param options Map of key value pairs to connect to data source
     * @return StepBuilder
     */
    public StepBuilder options(Map<String, String> options) {
        return new StepBuilder(scalaDef.options(toScalaMap(options)));
    }

    /**
     * Define table name to connect for JDBC data source.
     *
     * @param table Table name
     * @return StepBuilder
     */
    public StepBuilder jdbcTable(String table) {
        return new StepBuilder(scalaDef.jdbcTable(table));
    }

    /**
     * Define schema and table name for JDBC data source.
     *
     * @param schema Schema name
     * @param table  Table name
     * @return StepBuilder
     */
    public StepBuilder jdbcTable(String schema, String table) {
        return new StepBuilder(scalaDef.jdbcTable(schema, table));
    }

    /**
     * Keyspace and table name for Cassandra data source
     *
     * @param keyspace Keyspace name
     * @param table    Table name
     * @return StepBuilder
     */
    public StepBuilder cassandraTable(String keyspace, String table) {
        return new StepBuilder(scalaDef.cassandraTable(keyspace, table));
    }

    /**
     * The queue/topic name for a JMS data source.
     * This is used as part of connecting to a JMS destination as a JNDI resource
     *
     * @param destination Destination name
     * @return StepBuilder
     */
    public StepBuilder jmsDestination(String destination) {
        return new StepBuilder(scalaDef.jmsDestination(destination));
    }

    /**
     * Kafka topic to push data to for Kafka data source
     *
     * @param topic Topic name
     * @return StepBuilder
     */
    public StepBuilder kafkaTopic(String topic) {
        return new StepBuilder(scalaDef.kafkaTopic(topic));
    }

    /**
     * File pathway used for file data source.
     * Can be defined as a local file system path or cloud based path (i.e. s3a://my-bucket/file/path)
     *
     * @param path File path
     * @return StepBuilder
     */
    public StepBuilder path(String path) {
        return new StepBuilder(scalaDef.path(path));
    }

    /**
     * The columns within the generated data to use as partitions for a file data source.
     * Order of partition columns defined is used to define order of partitions.<br>
     * For example, {@code partitionBy("year", "account_id")`}
     * will ensure that `year` is used as the top level partition
     * before `account_id`.
     *
     * @param partitionBy  First partition column name
     * @param partitionsBy Other partition column names
     * @return StepBuilder
     */
    public StepBuilder partitionBy(String partitionBy, String... partitionsBy) {
        return new StepBuilder(scalaDef.partitionBy(partitionBy, toScalaSeq(Arrays.asList(partitionsBy))));
    }

    /**
     * Number of partitions to use when saving data to the data source.
     * This can be used to help fine tune performance depending on your data source.<br>
     * For example, if you are facing timeout errors when saving to your database, you can reduce the number of
     * partitions to help reduce the number of concurrent saves to your database.
     *
     * @param partitions Number of partitions when saving data to data source
     * @return StepBuilder
     */
    public StepBuilder numPartitions(int partitions) {
        return new StepBuilder(scalaDef.numPartitions(partitions));
    }

    /**
     * Number of rows pushed to data source per second.
     * Only used for real time data sources such as JMS, Kafka and HTTP.<br>
     * If you see that the number of rows per second is not reaching as high as expected, it may be due to the number
     * of partitions used when saving data. You will also need to increase the number of partitions via<br>
     * {@code .numPartitions(<my_value>)}
     *
     * @param rowsPerSecond Number of rows per second to generate
     * @return StepBuilder
     */
    public StepBuilder rowsPerSecond(int rowsPerSecond) {
        return new StepBuilder(scalaDef.rowsPerSecond(rowsPerSecond));
    }

    /**
     * Define number of records to be generated for the sub data source via CountBuilder
     *
     * @param countBuilder Configure number of records to generate
     * @return StepBuilder
     */
    public StepBuilder count(CountBuilder countBuilder) {
        return new StepBuilder(scalaDef.count(countBuilder.count()));
    }

    /**
     * Define total number of records to be generated.
     * If you also have defined a per column count, this value will not represent the full number of records generated.
     *
     * @param total Total number of records to generate
     * @return StepBuilder
     * @see <a href=https://pflooky.github.io/data-caterer-docs/setup/generator/count/>Count definition</a> for details
     */
    public StepBuilder count(long total) {
        return new StepBuilder(scalaDef.count(total));
    }

    /**
     * Define a generator to be used for determining the number of records to generate.
     * If you also have defined a per column count, the value generated will be combined with the per column count to
     * determine the total number of records
     *
     * @param generatorBuilder Generator builder for determining number of records to generate
     * @return StepBuilder
     * @see <a href=https://pflooky.github.io/data-caterer-docs/setup/generator/count/>Count definition</a> for details
     */
    public StepBuilder count(GeneratorBuilder generatorBuilder) {
        return new StepBuilder(scalaDef.count(generatorBuilder.generator()));
    }

    /**
     * Define the number of records to generate based off certain columns.<br>
     * For example, if you had a data set with columns account_id and amount, you can set that 10 records to be generated
     * per account_id via {@code .count(new PerColumnCountBuilder().total(10, "account_id")}.
     * The total number of records generated is also influenced by other count configurations.
     *
     * @param perColumnCountBuilder Per column count builder
     * @return StepBuilder
     * @see <a href=https://pflooky.github.io/data-caterer-docs/setup/generator/count/>Count definition</a> for details
     */
    public StepBuilder count(PerColumnCountBuilder perColumnCountBuilder) {
        return new StepBuilder(scalaDef.count(perColumnCountBuilder.perColumnCount()));
    }

    /**
     * Schema to use when generating data for data source.
     * The schema includes various metadata about each field to guide the data generator on what the data should look
     * like.
     *
     * @param schemaBuilder Schema builder
     * @return StepBuilder
     */
    public StepBuilder schema(SchemaBuilder schemaBuilder) {
        return new StepBuilder(scalaDef.schema(schemaBuilder.schema()));
    }

    /**
     * Define fields of the schema of the data source to use when generating data.
     *
     * @param field  First field of the schema
     * @param fields Other fields of the schema
     * @return StepBuilder
     */
    public StepBuilder schema(FieldBuilder field, FieldBuilder... fields) {
        return new StepBuilder(scalaDef.schema(field.field(), toScalaSeq(Arrays.stream(fields).map(FieldBuilder::field).collect(Collectors.toList()))));
    }
}
