package com.github.pflooky.datacaterer.api.java.model.config.connection;

import java.util.Optional;

public final class JdbcBuilder extends ConnectionTaskBuilder {

    public JdbcBuilder(com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder scalaDef) {
        super(scalaDef);
    }

    public JdbcBuilder table(String table) {
        setOptStep(Optional.of(getStep().jdbcTable(table)));
        return this;
    }

    public JdbcBuilder table(String schema, String table) {
        setOptStep(Optional.of(getStep().jdbcTable(schema, table)));
        return this;
    }
}
