package com.github.pflooky.datacaterer.api.java.model.validation;

import com.github.pflooky.datacaterer.api.model.Constants;
import com.github.pflooky.datacaterer.api.model.ValidationConfiguration;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaMap;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaSeq;

public final class ValidationConfigurationBuilder {

    private final com.github.pflooky.datacaterer.api.ValidationConfigurationBuilder scalaDef;

    public ValidationConfigurationBuilder(com.github.pflooky.datacaterer.api.ValidationConfigurationBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    public ValidationConfigurationBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.ValidationConfigurationBuilder(
                new ValidationConfiguration(
                        Constants.DEFAULT_VALIDATION_CONFIG_NAME(),
                        Constants.DEFAULT_VALIDATION_DESCRIPTION(),
                        toScalaMap(Collections.emptyMap())
                )
        );
    }

    public com.github.pflooky.datacaterer.api.ValidationConfigurationBuilder configuration() {
        return scalaDef;
    }

    public ValidationConfigurationBuilder name(String name) {
        return new ValidationConfigurationBuilder(scalaDef.name(name));
    }

    public ValidationConfigurationBuilder description(String description) {
        return new ValidationConfigurationBuilder(scalaDef.description(description));
    }

    public ValidationConfigurationBuilder addDataSourceValidation(
            String dataSourceName,
            DataSourceValidationBuilder dataSourceValidationBuilder
    ) {
        return new ValidationConfigurationBuilder(scalaDef.addDataSourceValidation(
                dataSourceName,
                dataSourceValidationBuilder.dataSourceValidation())
        );
    }

    public ValidationConfigurationBuilder addValidations(
            String dataSourceName,
            Map<String, String> options,
            ValidationBuilder validation,
            ValidationBuilder... validations
    ) {
        return new ValidationConfigurationBuilder(scalaDef.addValidationsJava(
                dataSourceName,
                toScalaMap(options),
                validation.validation(),
                toScalaSeq(Arrays.stream(validations).map(ValidationBuilder::validation).collect(Collectors.toList()))
        ));
    }

    public ValidationConfigurationBuilder addValidations(
            String dataSourceName,
            Map<String, String> options,
            WaitConditionBuilder waitConditionBuilder,
            ValidationBuilder validation,
            ValidationBuilder... validations
    ) {
        return new ValidationConfigurationBuilder(scalaDef.addValidationsJava(
                dataSourceName,
                toScalaMap(options),
                waitConditionBuilder.waitCondition(),
                validation.validation(),
                toScalaSeq(Arrays.stream(validations).map(ValidationBuilder::validation).collect(Collectors.toList()))
        ));
    }
}
