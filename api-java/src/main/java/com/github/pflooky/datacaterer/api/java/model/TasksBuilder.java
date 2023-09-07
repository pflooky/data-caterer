package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.model.Constants;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaList;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaSeq;

public final class TasksBuilder {

    private final com.github.pflooky.datacaterer.api.TasksBuilder scalaDef;

    public TasksBuilder(com.github.pflooky.datacaterer.api.TasksBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    public TasksBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.TasksBuilder(
                toScalaList(Collections.emptyList()),
                Constants.DEFAULT_DATA_SOURCE_NAME()
        );
    }

    public com.github.pflooky.datacaterer.api.TasksBuilder tasks() {
        return scalaDef;
    }

    public TasksBuilder addTasks(String dataSourceName, TaskBuilder... tasks) {
        return new TasksBuilder(scalaDef.addTasks(dataSourceName,
                toScalaSeq(Arrays.stream(tasks).map(TaskBuilder::task).collect(Collectors.toList()))
        ));
    }

    public TasksBuilder addTask(String name, String dataSourceName, StepBuilder... steps) {
        return new TasksBuilder(scalaDef.addTask(name, dataSourceName,
                toScalaList(Arrays.stream(steps).map(StepBuilder::step).collect(Collectors.toList()))
        ));
    }
}
