package com.github.pflooky.datacaterer.api.java.model.config.connection;

import java.util.Optional;

public final class SolaceBuilder extends ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.SolaceBuilder, SolaceBuilder> {

    public SolaceBuilder(com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.SolaceBuilder> scalaDef) {
        super(scalaDef);
    }

    @Override
    public SolaceBuilder fromBaseConfig(ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.SolaceBuilder, SolaceBuilder> connectionTaskBuilder) {
        this.setConnectionConfigWithTaskBuilder(connectionTaskBuilder.getConnectionConfigWithTaskBuilder());
        return this;
    }

    public SolaceBuilder destination(String destination) {
        setOptStep(Optional.of(getStep().jmsDestination(destination)));
        return this;
    }
}
