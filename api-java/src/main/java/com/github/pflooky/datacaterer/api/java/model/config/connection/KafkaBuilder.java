package com.github.pflooky.datacaterer.api.java.model.config.connection;

import java.util.Optional;

public final class KafkaBuilder extends ConnectionTaskBuilder {

    public KafkaBuilder(com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder scalaDef) {
        super(scalaDef);
    }

    public KafkaBuilder topic(String topic) {
        setOptStep(Optional.of(getStep().kafkaTopic(topic)));
        return this;
    }
}
