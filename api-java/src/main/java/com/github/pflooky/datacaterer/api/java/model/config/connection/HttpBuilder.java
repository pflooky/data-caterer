package com.github.pflooky.datacaterer.api.java.model.config.connection;

public final class HttpBuilder extends ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.HttpBuilder, HttpBuilder> {

    public HttpBuilder(com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.HttpBuilder> scalaDef) {
        super(scalaDef);
    }

    @Override
    public HttpBuilder fromBaseConfig(ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.HttpBuilder, HttpBuilder> connectionTaskBuilder) {
        this.setConnectionConfigWithTaskBuilder(connectionTaskBuilder.getConnectionConfigWithTaskBuilder());
        return this;
    }
}
