package com.github.pflooky.datacaterer.api.java.model.validation;

import com.github.pflooky.datacaterer.api.model.DataSourceValidation;
import com.github.pflooky.datacaterer.api.model.PauseWaitCondition;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaList;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaMap;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaSeq;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaTuple;

public final class DataSourceValidationBuilder {

    private final com.github.pflooky.datacaterer.api.DataSourceValidationBuilder scalaDef;

    public DataSourceValidationBuilder(com.github.pflooky.datacaterer.api.DataSourceValidationBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    public DataSourceValidationBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.DataSourceValidationBuilder(
                new DataSourceValidation(
                        toScalaMap(Collections.emptyMap()),
                        new PauseWaitCondition(0),
                        toScalaList(Collections.emptyList())
                )
        );
    }

    public com.github.pflooky.datacaterer.api.model.DataSourceValidation dataSourceValidation() {
        return scalaDef.dataSourceValidation();
    }

    public DataSourceValidationBuilder options(Map<String, String> options) {
        return new DataSourceValidationBuilder(scalaDef.options(toScalaMap(options)));
    }

    public DataSourceValidationBuilder option(String key, String value) {
        return new DataSourceValidationBuilder(scalaDef.option(toScalaTuple(key, value)));
    }

    public DataSourceValidationBuilder addValidation(ValidationBuilder validationBuilder) {
        return new DataSourceValidationBuilder(scalaDef.addValidation(validationBuilder.validation()));
    }

    public DataSourceValidationBuilder validations(ValidationBuilder... validationBuilders) {
        return new DataSourceValidationBuilder(scalaDef.validations(
                toScalaSeq(Arrays.stream(validationBuilders).map(ValidationBuilder::validation).collect(Collectors.toList())))
        );
    }

    public DataSourceValidationBuilder wait(WaitConditionBuilder waitConditionBuilder) {
        return new DataSourceValidationBuilder(scalaDef.wait(waitConditionBuilder.waitCondition()));
    }
}
