package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.model.Constants;
import com.github.pflooky.datacaterer.api.model.Count;

import java.util.Arrays;
import java.util.Optional;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaOption;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaSeq;

public final class CountBuilder {
    private final com.github.pflooky.datacaterer.api.CountBuilder scalaDef;

    public CountBuilder(com.github.pflooky.datacaterer.api.CountBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    public CountBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.CountBuilder(
                new Count(
                        toScalaOption(Optional.of(Constants.DEFAULT_COUNT_TOTAL())),
                        toScalaOption(Optional.empty()),
                        toScalaOption(Optional.empty())
                )
        );
    }

    public com.github.pflooky.datacaterer.api.model.Count count() {
        return scalaDef.count();
    }

    public CountBuilder total(long total) {
        return new CountBuilder(scalaDef.total(total));
    }

    public CountBuilder generator(GeneratorBuilder generatorBuilder) {
        return new CountBuilder(scalaDef.generator(generatorBuilder.generator()));
    }

    public CountBuilder perColumn(PerColumnCountBuilder perColumnCountBuilder) {
        return new CountBuilder(scalaDef.perColumn(perColumnCountBuilder.perColumnCount()));
    }

    public CountBuilder columns(String col, String... cols) {
        return new CountBuilder(scalaDef.columns(col, toScalaSeq(Arrays.asList(cols))));
    }

    public CountBuilder perColumnTotal(long total, String col, String... cols) {
        return new CountBuilder(scalaDef.perColumnTotal(total, col, toScalaSeq(Arrays.asList(cols))));
    }

    public CountBuilder perColumnGenerator(GeneratorBuilder generatorBuilder, String col, String... cols) {
        return new CountBuilder(scalaDef.perColumnGenerator(generatorBuilder.generator(), col, toScalaSeq(Arrays.asList(cols))));
    }

    public CountBuilder perColumnGeneratorWithTotal(long total, GeneratorBuilder generatorBuilder, String col, String... cols) {
        return new CountBuilder(scalaDef.perColumnGenerator(total, generatorBuilder.generator(), col, toScalaSeq(Arrays.asList(cols))));
    }
}
