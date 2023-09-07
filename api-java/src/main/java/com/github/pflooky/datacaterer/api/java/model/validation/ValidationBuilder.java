package com.github.pflooky.datacaterer.api.java.model.validation;

import com.github.pflooky.datacaterer.api.model.ExpressionValidation;

public final class ValidationBuilder {

    private final com.github.pflooky.datacaterer.api.ValidationBuilder scalaDef;

    public ValidationBuilder(com.github.pflooky.datacaterer.api.ValidationBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    public ValidationBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.ValidationBuilder(
                new ExpressionValidation("true")
        );
    }

    public com.github.pflooky.datacaterer.api.model.Validation validation() {
        return scalaDef.validation();
    }

    public ValidationBuilder description(String description) {
        return new ValidationBuilder(scalaDef.description(description));
    }

    public ValidationBuilder errorThreshold(double threshold) {
        return new ValidationBuilder(scalaDef.errorThreshold(threshold));
    }

    public ValidationBuilder expr(String expr) {
        return new ValidationBuilder(scalaDef.expr(expr));
    }
}
