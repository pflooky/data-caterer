package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.model.Constants;
import com.github.pflooky.datacaterer.api.model.PerColumnCount;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaList;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaOption;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaSeq;

/**
 * Define number of records to generate based on certain column values. This is used in situations where
 * you want to generate multiple records for a given set of column values to closer represent the real production
 * data setting. For example, you may have a data set containing bank transactions where you want to generate
 * multiple transactions per account.
 */
public class PerColumnCountBuilder {

    private final com.github.pflooky.datacaterer.api.PerColumnCountBuilder scalaDef;

    /**
     * Wrapper around Scala PerColumnCountBuilder
     *
     * @param scalaDef Scala PerColumnCountBuilder
     */
    public PerColumnCountBuilder(com.github.pflooky.datacaterer.api.PerColumnCountBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    /**
     * Default value for per column count include:<br>
     * - No column names defined
     * - 10 records to generate per set of columns defined
     * - No generator for determining number of records to generate
     */
    public PerColumnCountBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.PerColumnCountBuilder(
                new PerColumnCount(
                        toScalaList(Collections.emptyList()),
                        toScalaOption(Optional.of(Constants.DEFAULT_PER_COLUMN_COUNT_TOTAL())),
                        toScalaOption(Optional.empty())
                )
        );
    }

    /**
     * Build Scala PerColumnCount
     *
     * @return Scala PerColumnCount
     */
    public com.github.pflooky.datacaterer.api.model.PerColumnCount perColumnCount() {
        return scalaDef.perColumnCount();
    }

    /**
     * Number of records to generate per set of column values defined
     *
     * @param total Number of records
     * @param col   First column name
     * @param cols  Other column names
     * @return PerColumnCountBuilder
     */
    public PerColumnCountBuilder total(long total, String col, String... cols) {
        return new PerColumnCountBuilder(scalaDef.total(total, col, toScalaSeq(Arrays.asList(cols))));
    }

    /**
     * Define a generator to determine the number of records to generate per set of column value defined
     *
     * @param generatorBuilder Generator for number of records
     * @param col              First column name
     * @param cols             Other column names
     * @return PerColumnCountBuilder
     */
    public PerColumnCountBuilder generator(GeneratorBuilder generatorBuilder, String col, String... cols) {
        return new PerColumnCountBuilder(scalaDef.generator(generatorBuilder.generator(), col, toScalaSeq(Arrays.asList(cols))));
    }

    /**
     * Define the set of columns that should have multiple records generated for.
     *
     * @param col  First column name
     * @param cols Other column names
     * @return PerColumnCountBuilder
     */
    public PerColumnCountBuilder columns(String col, String... cols) {
        return new PerColumnCountBuilder(scalaDef.columns(col, toScalaSeq(Arrays.asList(cols))));
    }
}
