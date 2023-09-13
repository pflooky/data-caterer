package com.github.pflooky.datacaterer.api.java.model.config.connection;

import java.util.Optional;

public final class KafkaBuilder extends ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.KafkaBuilder, KafkaBuilder> {

    public KafkaBuilder(com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.KafkaBuilder> scalaDef) {
        super(scalaDef);
    }

    @Override
    public KafkaBuilder fromBaseConfig(ConnectionTaskBuilder<com.github.pflooky.datacaterer.api.connection.KafkaBuilder, KafkaBuilder> connectionTaskBuilder) {
        this.setConnectionConfigWithTaskBuilder(connectionTaskBuilder.getConnectionConfigWithTaskBuilder());
        return this;
    }

    public KafkaBuilder topic(String topic) {
        setOptStep(Optional.of(getStep().kafkaTopic(topic)));
        return this;
    }
}
