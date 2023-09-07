package com.github.pflooky.datacaterer.api.java.model.validation;

import com.github.pflooky.datacaterer.api.model.PauseWaitCondition;

import java.util.Map;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaMap;

public final class WaitConditionBuilder {

    private final com.github.pflooky.datacaterer.api.WaitConditionBuilder scalaDef;

    public WaitConditionBuilder(com.github.pflooky.datacaterer.api.WaitConditionBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    public WaitConditionBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.WaitConditionBuilder(
                new PauseWaitCondition(0)
        );
    }

    public com.github.pflooky.datacaterer.api.model.WaitCondition waitCondition() {
        return scalaDef.waitCondition();
    }

    public WaitConditionBuilder pause(int pauseInSeconds) {
        return new WaitConditionBuilder(scalaDef.pause(pauseInSeconds));
    }

    public WaitConditionBuilder file(String path) {
        return new WaitConditionBuilder(scalaDef.file(path));
    }

    public WaitConditionBuilder dataExists(String dataSourceName, Map<String, String> options, String expr) {
        return new WaitConditionBuilder(scalaDef.dataExists(dataSourceName, toScalaMap(options), expr));
    }

    public WaitConditionBuilder webhook(String dataSourceName, String url) {
        return new WaitConditionBuilder(scalaDef.webhook(dataSourceName, url));
    }

    public WaitConditionBuilder webhook(String dataSourceName, String url, String method, int statusCode) {
        return new WaitConditionBuilder(scalaDef.webhook(dataSourceName, url, method, statusCode));
    }
}
