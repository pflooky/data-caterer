package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.model.Constants;
import com.github.pflooky.datacaterer.api.model.Generator;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaMap;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaSeq;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaTuple;

/**
 * Data generator contains all the metadata, related to either a field or count generation, required to create new data.
 */
public final class GeneratorBuilder {
    private final com.github.pflooky.datacaterer.api.GeneratorBuilder scalaDef;

    /**
     * Wrapper for the Scala GeneratorBuilder
     *
     * @param scalaDef Scala GeneratorBuilder
     */
    public GeneratorBuilder(com.github.pflooky.datacaterer.api.GeneratorBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    /**
     * Default constructor creates a random generator with no metadata
     */
    public GeneratorBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.GeneratorBuilder(
                new Generator(Constants.DEFAULT_GENERATOR_TYPE(), toScalaMap(Collections.emptyMap()))
        );
    }

    /**
     * Scala definition of Generator for backend to run job
     *
     * @return GeneratorBuilder Scala Generator
     */
    public Generator generator() {
        return this.scalaDef.generator();
    }

    /**
     * Create a random data generator. Depending on the data type, particular defaults are set for the metadata
     *
     * @return GeneratorBuilder GeneratorBuilder
     * @see <a href="https://pflooky.github.io/data-caterer-docs/setup/generator/generator/">Data generator</a> default details here
     */
    public GeneratorBuilder random() {
        return new GeneratorBuilder(scalaDef.random());
    }

    /**
     * Create a SQL based generator. You can reference other columns and SQL functions to generate data. The output data
     * type from the SQL expression should also match the data type defined otherwise a runtime error will be thrown
     *
     * @param sql SQL expression
     * @return GeneratorBuilder
     */
    public GeneratorBuilder sql(String sql) {
        return new GeneratorBuilder(scalaDef.sql(sql));
    }

    /**
     * Create a generator based on a particular regex
     *
     * @param regex Regex data should adhere to
     * @return GeneratorBuilder
     */
    public GeneratorBuilder regex(String regex) {
        return new GeneratorBuilder(scalaDef.regex(regex));
    }

    /**
     * Create a generator that can only generate values from a set of values defined.
     *
     * @param values Set of valid values
     * @return GeneratorBuilder
     */
    public GeneratorBuilder oneOf(Object... values) {
        return new GeneratorBuilder(scalaDef.oneOf(toScalaSeq(Arrays.asList(values))));
    }

    /**
     * Define metadata for your generator. Add/overwrites existing metadata
     *
     * @param key   Key for metadata
     * @param value Value for metadata
     * @return GeneratorBuilder
     */
    public GeneratorBuilder option(String key, String value) {
        return new GeneratorBuilder(scalaDef.option(toScalaTuple(key, value)));
    }

    /**
     * Define metadata map for your generator. Add/overwrites existing metadata
     *
     * @param options Metadata map
     * @return GeneratorBuilder
     */
    public GeneratorBuilder options(Map<String, Object> options) {
        return new GeneratorBuilder(scalaDef.options(toScalaMap(options)));
    }

    /**
     * Seed to use for random generator. If you want to generate a consistent set of values, use this method
     *
     * @param seed Random seed
     * @return GeneratorBuilder
     */
    public GeneratorBuilder seed(long seed) {
        return new GeneratorBuilder(scalaDef.seed(seed));
    }

    /**
     * Enable/disable null values to be generated for this field
     *
     * @param enable Enable/disable null values
     * @return GeneratorBuilder
     */
    public GeneratorBuilder enableNull(boolean enable) {
        return new GeneratorBuilder(scalaDef.enableNull(enable));
    }

    /**
     * If {@link #enableNull(boolean)} is enabled, the generator will generate null values with the probability defined.
     * Value needs to be between 0.0 and 1.0.
     *
     * @param probability Probability of null values generated
     * @return GeneratorBuilder
     */
    public GeneratorBuilder nullProbability(double probability) {
        return new GeneratorBuilder(scalaDef.nullProbability(probability));
    }

    /**
     * Enable/disable edge case values to be generated. The edge cases are based on the data type defined.
     *
     * @param enable Enable/disable edge case values
     * @return GeneratorBuilder
     * @see <a href="https://pflooky.github.io/data-caterer-docs/setup/generator/generator/#options">Generator</a> details here
     */
    public GeneratorBuilder enableEdgeCases(boolean enable) {
        return new GeneratorBuilder(scalaDef.enableEdgeCases(enable));
    }

    /**
     * If {@link #enableEdgeCases(boolean)} is enabled, the generator will generate edge case values with the probability
     * defined. Value needs to be between 0.0 and 1.0.
     *
     * @param probability Probability of edge case values generated
     * @return GeneratorBuilder
     */
    public GeneratorBuilder edgeCaseProbability(double probability) {
        return new GeneratorBuilder(scalaDef.edgeCaseProbability(probability));
    }

    /**
     * Generator will always give back the static value, ignoring all other metadata defined
     *
     * @param value Always generate this value
     * @return GeneratorBuilder
     */
    public GeneratorBuilder staticValue(Object value) {
        return new GeneratorBuilder(scalaDef.staticValue(value));
    }

    /**
     * Unique values within the generated data will be generated. This does not take into account values already existing
     * in the data source defined. It also requires the flag
     * {@link com.github.pflooky.datacaterer.api.java.model.config.DataCatererConfigurationBuilder#enableUniqueCheck(boolean)}
     * to be enabled (disabled by default as it is an expensive operation).
     *
     * @param isUnique Enable/disable generating unique values
     * @return GeneratorBuilder
     */
    public GeneratorBuilder unique(boolean isUnique) {
        return new GeneratorBuilder(scalaDef.unique(isUnique));
    }

    /**
     * If data type is array, define the inner data type of the array
     *
     * @param type Type of array
     * @return GeneratorBuilder
     */
    public GeneratorBuilder arrayType(String type) {
        return new GeneratorBuilder(scalaDef.arrayType(type));
    }

    /**
     * Use a DataFaker expression to generate data. If you want to know what is possible to use as an expression, follow
     * the below link.
     *
     * @param expr DataFaker expression
     * @return GeneratorBuilder
     * @see <a href="https://pflooky.github.io/data-caterer-docs/setup/generator/generator/#string">Expression</a> details
     */
    public GeneratorBuilder expression(String expr) {
        return new GeneratorBuilder(scalaDef.expression(expr));
    }

    /**
     * Average length of data generated. Length is specifically used for String data type and is ignored for other data types
     *
     * @param length Average length
     * @return GeneratorBuilder
     */
    public GeneratorBuilder avgLength(int length) {
        return new GeneratorBuilder(scalaDef.avgLength(length));
    }

    /**
     * Minimum value to be generated. This can be used for any data type except for Struct and Array.
     *
     * @param min Minimum value
     * @return GeneratorBuilder
     */
    public GeneratorBuilder min(Object min) {
        return new GeneratorBuilder(scalaDef.min(min));
    }

    /**
     * Minimum length of data generated. Length is specifically used for String data type and is ignored for other data types
     *
     * @param length Minimum length
     * @return GeneratorBuilder
     */
    public GeneratorBuilder minLength(int length) {
        return new GeneratorBuilder(scalaDef.minLength(length));
    }

    /**
     * Minimum length of array generated. Only used when data type is Array
     *
     * @param length Minimum length of array
     * @return GeneratorBuilder
     */
    public GeneratorBuilder arrayMinLength(int length) {
        return new GeneratorBuilder(scalaDef.arrayMinLength(length));
    }

    /**
     * Maximum value to be generated. This can be used for any data type except for Struct and Array. Can be ignored in
     * scenario where database column is auto increment where values generated start from the max value.
     *
     * @param max Maximum value
     * @return GeneratorBuilder
     */
    public GeneratorBuilder max(Object max) {
        return new GeneratorBuilder(scalaDef.max(max));
    }

    /**
     * Maximum length of data generated. Length is specifically used for String data type and is ignored for other data types
     *
     * @param length Maximum length
     * @return GeneratorBuilder
     */
    public GeneratorBuilder maxLength(int length) {
        return new GeneratorBuilder(scalaDef.maxLength(length));
    }

    /**
     * Maximum length of array generated. Only used when data type is Array
     *
     * @param length Maximum length of array
     * @return GeneratorBuilder
     */
    public GeneratorBuilder arrayMaxLength(int length) {
        return new GeneratorBuilder(scalaDef.arrayMaxLength(length));
    }

    /**
     * Numeric precision used for Decimal data type
     *
     * @param precision Decimal precision
     * @return GeneratorBuilder
     */
    public GeneratorBuilder numericPrecision(int precision) {
        return new GeneratorBuilder(scalaDef.numericPrecision(precision));
    }

    /**
     * Numeric scale for Decimal data type
     *
     * @param scale Decimal scale
     * @return GeneratorBuilder
     */
    public GeneratorBuilder numericScale(int scale) {
        return new GeneratorBuilder(scalaDef.numericScale(scale));
    }

    /**
     * Enable/disable including the value in the final output to the data source. Allows you to define intermediate values
     * that can be used to generate other columns
     *
     * @param omit Enable/disable the value being in output to data source
     * @return GeneratorBuilder
     */
    public GeneratorBuilder omit(boolean omit) {
        return new GeneratorBuilder(scalaDef.omit(omit));
    }

    /**
     * Field is a primary key of the data source.
     *
     * @param isPrimaryKey Enable/disable field being a primary key
     * @return GeneratorBuilder
     */
    public GeneratorBuilder primaryKey(boolean isPrimaryKey) {
        return new GeneratorBuilder(scalaDef.primaryKey(isPrimaryKey));
    }

    /**
     * If {@link #primaryKey(boolean)} is enabled, this defines the position of the primary key. Starts at 1.
     *
     * @param position Position of primary key
     * @return GeneratorBuilder
     */
    public GeneratorBuilder primaryKeyPosition(int position) {
        return new GeneratorBuilder(scalaDef.primaryKeyPosition(position));
    }

    /**
     * If the data source supports clustering order (like Cassandra), this represents the order of the clustering key.
     * Starts at 1.
     *
     * @param position Position of clustering key
     * @return GeneratorBuilder
     */
    public GeneratorBuilder clusteringPosition(int position) {
        return new GeneratorBuilder(scalaDef.clusteringPosition(position));
    }
}
