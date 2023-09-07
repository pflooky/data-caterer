package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.model.Constants;
import com.github.pflooky.datacaterer.api.model.TaskSummary;

import java.util.Optional;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaOption;

public final class TaskSummaryBuilder {
    private final com.github.pflooky.datacaterer.api.TaskSummaryBuilder scalaDef;

    public TaskSummaryBuilder(com.github.pflooky.datacaterer.api.TaskSummaryBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    public TaskSummaryBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.TaskSummaryBuilder(
                new TaskSummary(Constants.DEFAULT_TASK_NAME(), Constants.DEFAULT_DATA_SOURCE_NAME(), true),
                toScalaOption(Optional.empty())
        );
    }

    public com.github.pflooky.datacaterer.api.TaskSummaryBuilder taskSummary() {
        return new com.github.pflooky.datacaterer.api.TaskSummaryBuilder(scalaDef.taskSummary(), scalaDef.task());
    }

    public TaskSummaryBuilder name(String name) {
        return new TaskSummaryBuilder(scalaDef.name(name));
    }

    public TaskSummaryBuilder task(TaskBuilder taskBuilder) {
        return new TaskSummaryBuilder(scalaDef.task(taskBuilder.task()));
    }

    public TaskSummaryBuilder dataSource(String name) {
        return new TaskSummaryBuilder(scalaDef.dataSource(name));
    }

    public TaskSummaryBuilder enabled(boolean enabled) {
        return new TaskSummaryBuilder(scalaDef.enabled(enabled));
    }
}
