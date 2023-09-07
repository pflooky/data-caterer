package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.java.SchemaBuilder;
import com.github.pflooky.datacaterer.api.model.Constants;
import com.github.pflooky.datacaterer.api.model.DataType;
import com.github.pflooky.datacaterer.api.model.Field;
import com.github.pflooky.datacaterer.api.model.Generator;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaMap;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaOption;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaSeq;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaTuple;

public final class FieldBuilder {
    private final com.github.pflooky.datacaterer.api.FieldBuilder scalaDef;

    public FieldBuilder(com.github.pflooky.datacaterer.api.FieldBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    public FieldBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.FieldBuilder(
                new Field(
                        Constants.DEFAULT_FIELD_NAME(),
                        toScalaOption(Optional.of("string")),
                        toScalaOption(Optional.of(new GeneratorBuilder().generator())),
                        Constants.DEFAULT_FIELD_NULLABLE(),
                        toScalaOption(Optional.empty()),
                        toScalaOption(Optional.empty())
                )
        );
    }

    public com.github.pflooky.datacaterer.api.model.Field field() {
        return scalaDef.field();
    }

    public FieldBuilder name(String name) {
        return new FieldBuilder(scalaDef.name(name));
    }

    public FieldBuilder type(DataType type) {
        return new FieldBuilder(scalaDef.type(type));
    }

    public FieldBuilder schema(SchemaBuilder schema) {
        return new FieldBuilder(scalaDef.schema(schema.schema()));
    }

    public FieldBuilder nullable(boolean nullable) {
        return new FieldBuilder(scalaDef.nullable(nullable));
    }

    public FieldBuilder generator(GeneratorBuilder generatorBuilder) {
        return new FieldBuilder(scalaDef.generator(generatorBuilder.generator()));
    }

    public FieldBuilder random() {
        return new FieldBuilder(scalaDef.random());
    }

    public FieldBuilder sql(String sql) {
        return new FieldBuilder(scalaDef.sql(sql));
    }

    public FieldBuilder regex(String regex) {
        return new FieldBuilder(scalaDef.regex(regex));
    }

    public FieldBuilder oneOf(Object... values) {
        return new FieldBuilder(scalaDef.oneOf(toScalaSeq(Arrays.asList(values))));
    }

    public FieldBuilder options(Map<String, Object> options) {
        return new FieldBuilder(scalaDef.options(toScalaMap(options)));
    }

    public FieldBuilder option(String key, String value) {
        return new FieldBuilder(scalaDef.option(toScalaTuple(key, value)));
    }

    public FieldBuilder seed(long seed) {
        return new FieldBuilder(scalaDef.seed(seed));
    }

    public FieldBuilder enableNull(boolean enable) {
        return new FieldBuilder(scalaDef.enableNull(enable));
    }

    public FieldBuilder nullProbability(double probability) {
        return new FieldBuilder(scalaDef.nullProbability(probability));
    }

    public FieldBuilder enableEdgeCases(boolean enable) {
        return new FieldBuilder(scalaDef.enableEdgeCases(enable));
    }

    public FieldBuilder edgeCaseProbability(double probability) {
        return new FieldBuilder(scalaDef.edgeCaseProbability(probability));
    }

    public FieldBuilder staticValue(Object value) {
        return new FieldBuilder(scalaDef.staticValue(value));
    }

    public FieldBuilder unique(boolean isUnique) {
        return new FieldBuilder(scalaDef.unique(isUnique));
    }

    public FieldBuilder arrayType(String type) {
        return new FieldBuilder(scalaDef.arrayType(type));
    }

    public FieldBuilder expression(String expr) {
        return new FieldBuilder(scalaDef.expression(expr));
    }

    public FieldBuilder avgLength(int length) {
        return new FieldBuilder(scalaDef.avgLength(length));
    }

    public FieldBuilder min(Object min) {
        return new FieldBuilder(scalaDef.min(min));
    }

    public FieldBuilder minLength(int minLength) {
        return new FieldBuilder(scalaDef.minLength(minLength));
    }

    public FieldBuilder listMinLength(int minLength) {
        return new FieldBuilder(scalaDef.listMinLength(minLength));
    }

    public FieldBuilder max(Object max) {
        return new FieldBuilder(scalaDef.max(max));
    }

    public FieldBuilder maxLength(int maxLength) {
        return new FieldBuilder(scalaDef.maxLength(maxLength));
    }

    public FieldBuilder listMaxLength(int maxLength) {
        return new FieldBuilder(scalaDef.listMaxLength(maxLength));
    }

    public FieldBuilder numericPrecision(int precision) {
        return new FieldBuilder(scalaDef.numericPrecision(precision));
    }

    public FieldBuilder numericScale(int scale) {
        return new FieldBuilder(scalaDef.numericScale(scale));
    }

    public FieldBuilder omit(boolean omit) {
        return new FieldBuilder(scalaDef.omit(omit));
    }

    public FieldBuilder primaryKey(boolean isPrimaryKey) {
        return new FieldBuilder(scalaDef.primaryKey(isPrimaryKey));
    }

    public FieldBuilder primaryKeyPosition(int position) {
        return new FieldBuilder(scalaDef.primaryKeyPosition(position));
    }

    public FieldBuilder clusteringPosition(int position) {
        return new FieldBuilder(scalaDef.clusteringPosition(position));
    }
}
