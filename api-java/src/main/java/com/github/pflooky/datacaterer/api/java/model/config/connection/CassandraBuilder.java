package com.github.pflooky.datacaterer.api.java.model.config.connection;

import java.util.Optional;

public final class CassandraBuilder extends ConnectionTaskBuilder {

    public CassandraBuilder(com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder scalaDef) {
        super(scalaDef);
    }

    public CassandraBuilder table(String keyspace, String table) {
        setOptStep(Optional.of(getStep().cassandraTable(keyspace, table)));
        return this;
    }
}
