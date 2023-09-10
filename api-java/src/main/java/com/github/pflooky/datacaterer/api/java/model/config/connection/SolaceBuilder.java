package com.github.pflooky.datacaterer.api.java.model.config.connection;

import java.util.Optional;

public final class SolaceBuilder extends ConnectionTaskBuilder {

    public SolaceBuilder(com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder scalaDef) {
        super(scalaDef);
    }

    public SolaceBuilder destination(String destination) {
        setOptStep(Optional.of(getStep().jmsDestination(destination)));
        return this;
    }
}
