package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.model.Constants;
import com.github.pflooky.datacaterer.api.model.Generator;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaMap;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaSeq;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaTuple;

public final class GeneratorBuilder {
    private final com.github.pflooky.datacaterer.api.GeneratorBuilder scalaDef;

    public GeneratorBuilder(com.github.pflooky.datacaterer.api.GeneratorBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    public GeneratorBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.GeneratorBuilder(
                new Generator(Constants.DEFAULT_GENERATOR_TYPE(), toScalaMap(Collections.emptyMap()))
        );
    }

    public Generator generator() {
        return this.scalaDef.generator();
    }

    public GeneratorBuilder random() {
        return new GeneratorBuilder(scalaDef.random());
    }

    public GeneratorBuilder sql(String sql) {
        return new GeneratorBuilder(scalaDef.sql(sql));
    }

    public GeneratorBuilder regex(String regex) {
        return new GeneratorBuilder(scalaDef.regex(regex));
    }

    public GeneratorBuilder oneOf(Object... values) {
        return new GeneratorBuilder(scalaDef.oneOf(toScalaSeq(Arrays.asList(values))));
    }

    public GeneratorBuilder option(String key, String value) {
        return new GeneratorBuilder(scalaDef.option(toScalaTuple(key, value)));
    }

    public GeneratorBuilder options(Map<String, Object> options) {
        return new GeneratorBuilder(scalaDef.options(toScalaMap(options)));
    }

    public GeneratorBuilder seed(long seed) {
        return new GeneratorBuilder(scalaDef.seed(seed));
    }

    public GeneratorBuilder enableNull(boolean enable) {
        return new GeneratorBuilder(scalaDef.enableNull(enable));
    }

    public GeneratorBuilder nullProbability(double probability) {
        return new GeneratorBuilder(scalaDef.nullProbability(probability));
    }

    public GeneratorBuilder enableEdgeCases(boolean enable) {
        return new GeneratorBuilder(scalaDef.enableEdgeCases(enable));
    }

    public GeneratorBuilder edgeCaseProbability(double probability) {
        return new GeneratorBuilder(scalaDef.edgeCaseProbability(probability));
    }

    public GeneratorBuilder staticValue(Object value) {
        return new GeneratorBuilder(scalaDef.staticValue(value));
    }

    public GeneratorBuilder unique(boolean isUnique) {
        return new GeneratorBuilder(scalaDef.unique(isUnique));
    }

    public GeneratorBuilder arrayType(String type) {
        return new GeneratorBuilder(scalaDef.arrayType(type));
    }

    public GeneratorBuilder expression(String expr) {
        return new GeneratorBuilder(scalaDef.expression(expr));
    }

    public GeneratorBuilder avgLength(int length) {
        return new GeneratorBuilder(scalaDef.avgLength(length));
    }

    public GeneratorBuilder min(int min) {
        return new GeneratorBuilder(scalaDef.min(min));
    }

    public GeneratorBuilder minLength(int length) {
        return new GeneratorBuilder(scalaDef.minLength(length));
    }

    public GeneratorBuilder listMinLength(int length) {
        return new GeneratorBuilder(scalaDef.listMinLength(length));
    }

    public GeneratorBuilder max(int max) {
        return new GeneratorBuilder(scalaDef.max(max));
    }

    public GeneratorBuilder maxLength(int length) {
        return new GeneratorBuilder(scalaDef.maxLength(length));
    }

    public GeneratorBuilder listMaxLength(int length) {
        return new GeneratorBuilder(scalaDef.listMaxLength(length));
    }

    public GeneratorBuilder numericPrecision(int precision) {
        return new GeneratorBuilder(scalaDef.numericPrecision(precision));
    }

    public GeneratorBuilder numericScale(int scale) {
        return new GeneratorBuilder(scalaDef.numericScale(scale));
    }

    public GeneratorBuilder omit(boolean omit) {
        return new GeneratorBuilder(scalaDef.omit(omit));
    }

    public GeneratorBuilder primaryKey(boolean isPrimaryKey) {
        return new GeneratorBuilder(scalaDef.primaryKey(isPrimaryKey));
    }

    public GeneratorBuilder primaryKeyPosition(int position) {
        return new GeneratorBuilder(scalaDef.primaryKeyPosition(position));
    }

    public GeneratorBuilder clusteringPosition(int position) {
        return new GeneratorBuilder(scalaDef.clusteringPosition(position));
    }
}
