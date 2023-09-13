package com.github.pflooky.datacaterer.api.java.model.config.connection;

import java.util.Optional;

public abstract class JdbcBuilder<T, K> extends ConnectionTaskBuilder<T, K> {

    public JdbcBuilder(com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<T> scalaDef) {
        super(scalaDef);
    }

    public JdbcBuilder<T, K> table(String table) {
        setOptStep(Optional.of(getStep().jdbcTable(table)));
        return this;
    }

    public JdbcBuilder<T, K> table(String schema, String table) {
        setOptStep(Optional.of(getStep().jdbcTable(schema, table)));
        return this;
    }
}
