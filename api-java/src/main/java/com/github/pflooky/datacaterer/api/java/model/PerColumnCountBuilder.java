package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.model.Constants;
import com.github.pflooky.datacaterer.api.model.PerColumnCount;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaList;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaOption;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaSeq;

public class PerColumnCountBuilder {

    private final com.github.pflooky.datacaterer.api.PerColumnCountBuilder scalaDef;

    public PerColumnCountBuilder(com.github.pflooky.datacaterer.api.PerColumnCountBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    public PerColumnCountBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.PerColumnCountBuilder(
                new PerColumnCount(
                        toScalaList(Collections.emptyList()),
                        toScalaOption(Optional.of(Constants.DEFAULT_PER_COLUMN_COUNT_TOTAL())),
                        toScalaOption(Optional.empty())
                )
        );
    }

    public com.github.pflooky.datacaterer.api.model.PerColumnCount perColumnCount() {
        return scalaDef.perColumnCount();
    }

    public PerColumnCountBuilder total(long total, String col, String... cols) {
        return new PerColumnCountBuilder(scalaDef.total(total, col, toScalaSeq(Arrays.asList(cols))));
    }

    public PerColumnCountBuilder generator(GeneratorBuilder generatorBuilder, String col, String... cols) {
        return new PerColumnCountBuilder(scalaDef.generator(generatorBuilder.generator(), col, toScalaSeq(Arrays.asList(cols))));
    }

    public PerColumnCountBuilder columns(String col, String... cols) {
        return new PerColumnCountBuilder(scalaDef.columns(col, toScalaSeq(Arrays.asList(cols))));
    }
}
