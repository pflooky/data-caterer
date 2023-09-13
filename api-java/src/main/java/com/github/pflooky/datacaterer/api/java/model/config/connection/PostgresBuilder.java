package com.github.pflooky.datacaterer.api.java.model.config.connection;


public class PostgresBuilder extends JdbcBuilder<com.github.pflooky.datacaterer.api.connection.PostgresBuilder, PostgresBuilder> {

    public PostgresBuilder(com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.PostgresBuilder> scalaDef) {
        super(scalaDef);
    }

    @Override
    public PostgresBuilder fromBaseConfig(ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.PostgresBuilder, PostgresBuilder> connectionTaskBuilder) {
        this.setConnectionConfigWithTaskBuilder(connectionTaskBuilder.getConnectionConfigWithTaskBuilder());
        return this;
    }
}
