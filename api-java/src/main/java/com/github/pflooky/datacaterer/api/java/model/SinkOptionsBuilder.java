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

public final class SinkOptionsBuilder {
    private final com.github.pflooky.datacaterer.api.SinkOptionsBuilder scalaDef;

    public SinkOptionsBuilder(com.github.pflooky.datacaterer.api.SinkOptionsBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    public SinkOptionsBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.SinkOptionsBuilder(
                new SinkOptions(
                        toScalaOption(Optional.empty()),
                        toScalaOption(Optional.empty()),
                        toScalaMap(Collections.emptyMap())
                )
        );
    }

    public com.github.pflooky.datacaterer.api.model.SinkOptions sinkOptions() {
        return scalaDef.sinkOptions();
    }

    public SinkOptionsBuilder seed(long seed) {
        return new SinkOptionsBuilder(scalaDef.seed(seed));
    }

    public SinkOptionsBuilder locale(String locale) {
        return new SinkOptionsBuilder(scalaDef.locale(locale));
    }

    public SinkOptionsBuilder foreignKey(ForeignKeyRelation foreignKeyRelation, ForeignKeyRelation relation, ForeignKeyRelation... relations) {
        return new SinkOptionsBuilder(scalaDef.foreignKey(foreignKeyRelation, relation, toScalaSeq(Arrays.asList(relations))));
    }

    public SinkOptionsBuilder foreignKey(ForeignKeyRelation foreignKeyRelation, List<ForeignKeyRelation> relations) {
        return new SinkOptionsBuilder(scalaDef.foreignKey(foreignKeyRelation, toScalaList(relations)));
    }
}
