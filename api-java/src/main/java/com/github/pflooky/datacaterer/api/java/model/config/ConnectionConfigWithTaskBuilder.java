package com.github.pflooky.datacaterer.api.java.model.config;

import com.github.pflooky.datacaterer.api.java.model.config.connection.CassandraBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.connection.FileBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.connection.HttpBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.connection.KafkaBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.connection.MySqlBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.connection.PostgresBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.connection.SolaceBuilder;
import com.github.pflooky.datacaterer.api.model.Constants;

import java.util.Collections;
import java.util.Map;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaMap;

public final class ConnectionConfigWithTaskBuilder {
    private final com.github.pflooky.datacaterer.api.ConnectionConfigWithTaskBuilder scalaDef;

    public ConnectionConfigWithTaskBuilder(com.github.pflooky.datacaterer.api.ConnectionConfigWithTaskBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    public ConnectionConfigWithTaskBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.ConnectionConfigWithTaskBuilder(
                Constants.DEFAULT_DATA_SOURCE_NAME(),
                toScalaMap(Collections.emptyMap())
        );
    }

    public com.github.pflooky.datacaterer.api.ConnectionConfigWithTaskBuilder connectionConfig() {
        return scalaDef;
    }

    public FileBuilder file(
            String name,
            String format,
            String path,
            Map<String, String> options
    ) {
        return new FileBuilder(scalaDef.file(name, format, path, toScalaMap(options)));
    }

    public PostgresBuilder postgres(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return new PostgresBuilder(scalaDef.postgres(name, url, username, password, toScalaMap(options)));
    }

    public MySqlBuilder mysql(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return new MySqlBuilder(scalaDef.mySql(name, url, username, password, toScalaMap(options)));
    }

    public CassandraBuilder cassandra(
            String name,
            String url,
            String username,
            String password,
            Map<String, String> options
    ) {
        return new CassandraBuilder(scalaDef.cassandra(
                name, url, username, password, toScalaMap(options)
        ));
    }

    public SolaceBuilder solace(
            String name,
            String url,
            String username,
            String password,
            String vpnName,
            String connectionFactory,
            String initialContextFactory,
            Map<String, String> options
    ) {
        return new SolaceBuilder(scalaDef.solace(
                name, url, username, password, vpnName, connectionFactory, initialContextFactory, toScalaMap(options))
        );
    }

    public KafkaBuilder kafka(
            String name,
            String url,
            Map<String, String> options
    ) {
        return new KafkaBuilder(scalaDef.kafka(name, url, toScalaMap(options)));
    }

    public HttpBuilder http(
            String name,
            String username,
            String password,
            Map<String, String> options
    ) {
        return new HttpBuilder(scalaDef.http(name, username, password, toScalaMap(options)));
    }
}
