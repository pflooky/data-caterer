package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.java.SchemaBuilder;
import com.github.pflooky.datacaterer.api.model.Constants;
import com.github.pflooky.datacaterer.api.model.Count;
import com.github.pflooky.datacaterer.api.model.Schema;
import com.github.pflooky.datacaterer.api.model.Step;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaMap;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaOption;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaSeq;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaTuple;

public final class StepBuilder {
    private final com.github.pflooky.datacaterer.api.StepBuilder scalaDef;

    public StepBuilder(com.github.pflooky.datacaterer.api.StepBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

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

    public com.github.pflooky.datacaterer.api.model.Step step() {
        return scalaDef.step();
    }

    public StepBuilder name(String name) {
        return new StepBuilder(scalaDef.name(name));
    }

    public StepBuilder type(String type) {
        return new StepBuilder(scalaDef.type(type));
    }

    public StepBuilder enabled(boolean enabled) {
        return new StepBuilder(scalaDef.enabled(enabled));
    }

    public StepBuilder option(String key, String value) {
        return new StepBuilder(scalaDef.option(toScalaTuple(key, value)));
    }

    public StepBuilder options(Map<String, String> options) {
        return new StepBuilder(scalaDef.options(toScalaMap(options)));
    }

    public StepBuilder jdbcTable(String table) {
        return new StepBuilder(scalaDef.jdbcTable(table));
    }

    public StepBuilder jdbcTable(String schema, String table) {
        return new StepBuilder(scalaDef.jdbcTable(schema, table));
    }

    public StepBuilder cassandraTable(String keyspace, String table) {
        return new StepBuilder(scalaDef.cassandraTable(keyspace, table));
    }

    public StepBuilder jmsDestination(String destination) {
        return new StepBuilder(scalaDef.jmsDestination(destination));
    }

    public StepBuilder kafkaTopic(String topic) {
        return new StepBuilder(scalaDef.kafkaTopic(topic));
    }

    public StepBuilder path(String path) {
        return new StepBuilder(scalaDef.path(path));
    }

    public StepBuilder partitionBy(String... partitionBy) {
        return new StepBuilder(scalaDef.partitionBy(toScalaSeq(Arrays.asList(partitionBy))));
    }

    public StepBuilder numPartitions(int partitions) {
        return new StepBuilder(scalaDef.numPartitions(partitions));
    }

    public StepBuilder rowsPerSecond(int rowsPerSecond) {
        return new StepBuilder(scalaDef.rowsPerSecond(rowsPerSecond));
    }

    public StepBuilder count(CountBuilder countBuilder) {
        return new StepBuilder(scalaDef.count(countBuilder.count()));
    }

    public StepBuilder count(long total) {
        return new StepBuilder(scalaDef.count(total));
    }

    public StepBuilder count(GeneratorBuilder generatorBuilder) {
        return new StepBuilder(scalaDef.count(generatorBuilder.generator()));
    }

    public StepBuilder count(PerColumnCountBuilder perColumnCountBuilder) {
        return new StepBuilder(scalaDef.count(perColumnCountBuilder.perColumnCount()));
    }

    public StepBuilder schema(SchemaBuilder schemaBuilder) {
        return new StepBuilder(scalaDef.schema(schemaBuilder.schema()));
    }
}
