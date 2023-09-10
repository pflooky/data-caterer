package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.model.ForeignKeyRelation;
import com.github.pflooky.datacaterer.api.model.SinkOptions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaList;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaMap;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaOption;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaSeq;

/**
 * Configurations that get applied across all generated data. This includes the random seed value, locale and foreign keys
 */
public final class SinkOptionsBuilder {
    private final com.github.pflooky.datacaterer.api.SinkOptionsBuilder scalaDef;

    /**
     * Wrapper around Scala definition of SinkOptionsBuilder
     *
     * @param scalaDef Scala SinkOptionsBuilder
     */
    public SinkOptionsBuilder(com.github.pflooky.datacaterer.api.SinkOptionsBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    /**
     * Default constructor defines no random seed, no locale (default to 'en') and no foreign keys
     */
    public SinkOptionsBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.SinkOptionsBuilder(
                new SinkOptions(
                        toScalaOption(Optional.empty()),
                        toScalaOption(Optional.empty()),
                        toScalaMap(Collections.emptyMap())
                )
        );
    }

    /**
     * Build Scala SinkOptions
     *
     * @return Scala SinkOptions
     */
    public com.github.pflooky.datacaterer.api.model.SinkOptions sinkOptions() {
        return scalaDef.sinkOptions();
    }

    /**
     * Random seed value to be used across all generated data
     *
     * @param seed Used as seed argument when creating Random instance
     * @return SinkOptionsBuilder
     */
    public SinkOptionsBuilder seed(long seed) {
        return new SinkOptionsBuilder(scalaDef.seed(seed));
    }

    /**
     * Locale used when generating data via DataFaker expressions
     *
     * @param locale Locale for DataFaker data generated
     * @return SinkOptionsBuilder
     * @see <a href="https://pflooky.github.io/data-caterer-docs/setup/generator/generator/#string">Docs</a> for details
     */
    public SinkOptionsBuilder locale(String locale) {
        return new SinkOptionsBuilder(scalaDef.locale(locale));
    }

    /**
     * Define a foreign key relationship between columns across any data source.
     * To define which column to use, it is defined by the following:<br>
     * dataSourceName + stepName + columnName
     *
     * @param foreignKeyRelation Base foreign key
     * @param relation           First foreign key relation
     * @param relations          Other foreign key relations
     * @return SinkOptionsBuilder
     * @see <a href="https://pflooky.github.io/data-caterer-docs/advanced/advanced/#foreign-keys-across-data-sets">Docs</a> for details
     */
    public SinkOptionsBuilder foreignKey(ForeignKeyRelation foreignKeyRelation, ForeignKeyRelation relation, ForeignKeyRelation... relations) {
        return new SinkOptionsBuilder(scalaDef.foreignKey(foreignKeyRelation, relation, toScalaSeq(Arrays.asList(relations))));
    }

    /**
     * Define a foreign key relationship between columns across any data source.
     * To define which column to use, it is defined by the following:<br>
     * dataSourceName + stepName + columnName
     *
     * @param foreignKeyRelation Base foreign key
     * @param relations          Other foreign key relations
     * @return SinkOptionsBuilder
     * @see <a href="https://pflooky.github.io/data-caterer-docs/advanced/advanced/#foreign-keys-across-data-sets">Docs</a> for details
     */
    public SinkOptionsBuilder foreignKey(ForeignKeyRelation foreignKeyRelation, List<ForeignKeyRelation> relations) {
        return new SinkOptionsBuilder(scalaDef.foreignKey(foreignKeyRelation, toScalaList(relations)));
    }
}
