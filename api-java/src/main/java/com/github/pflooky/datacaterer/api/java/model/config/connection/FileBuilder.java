package com.github.pflooky.datacaterer.api.java.model.config.connection;

import java.util.Optional;

public class FileBuilder extends ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.FileBuilder, FileBuilder> {

    public FileBuilder(com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.FileBuilder> scalaDef) {
        super(scalaDef);
    }

    @Override
    public FileBuilder fromBaseConfig(ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.FileBuilder, FileBuilder> connectionTaskBuilder) {
        this.setConnectionConfigWithTaskBuilder(connectionTaskBuilder.getConnectionConfigWithTaskBuilder());
        return this;
    }

    public FileBuilder partitionBy(String partitionBy, String... partitionsBy) {
        setOptStep(Optional.of(getStep().partitionBy(partitionBy, partitionsBy)));
        return this;
    }
}
