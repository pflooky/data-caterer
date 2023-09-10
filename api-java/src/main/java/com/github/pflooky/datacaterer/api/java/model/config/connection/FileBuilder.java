package com.github.pflooky.datacaterer.api.java.model.config.connection;

import java.util.Optional;

public class FileBuilder extends ConnectionTaskBuilder {

    public FileBuilder(com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder scalaDef) {
        super(scalaDef);
    }

    public FileBuilder partitionBy(String partitionBy, String... partitionsBy) {
        setOptStep(Optional.of(getStep().partitionBy(partitionBy, partitionsBy)));
        return this;
    }
}
