package com.github.pflooky.datacaterer.api.java.model.config.connection;

import java.util.Optional;

public final class CassandraBuilder extends ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.CassandraBuilder, CassandraBuilder> {

    public CassandraBuilder(com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.CassandraBuilder> scalaDef) {
        super(scalaDef);
    }

    @Override
    public CassandraBuilder fromBaseConfig(ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.CassandraBuilder, CassandraBuilder> connectionTaskBuilder) {
        this.setConnectionConfigWithTaskBuilder(connectionTaskBuilder.getConnectionConfigWithTaskBuilder());
        return this;
    }

    public CassandraBuilder table(String keyspace, String table) {
        setOptStep(Optional.of(getStep().cassandraTable(keyspace, table)));
        return this;
    }
}
