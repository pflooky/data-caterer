package com.github.pflooky.datacaterer.api.java.model.config.connection;


public class MySqlBuilder extends JdbcBuilder<com.github.pflooky.datacaterer.api.connection.MySqlBuilder, MySqlBuilder> {

    public MySqlBuilder(com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.MySqlBuilder> scalaDef) {
        super(scalaDef);
    }

    @Override
    public MySqlBuilder fromBaseConfig(ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.MySqlBuilder, MySqlBuilder> connectionTaskBuilder) {
        this.setConnectionConfigWithTaskBuilder(connectionTaskBuilder.getConnectionConfigWithTaskBuilder());
        return this;
    }
}
