package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.model.Constants;
import com.github.pflooky.datacaterer.api.model.Task;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaList;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaSeq;

public final class TaskBuilder {

    private final com.github.pflooky.datacaterer.api.TaskBuilder scalaDef;

    public TaskBuilder(com.github.pflooky.datacaterer.api.TaskBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    public TaskBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.TaskBuilder(
                new Task(Constants.DEFAULT_TASK_NAME(), toScalaList(Collections.emptyList()))
        );
    }

    public com.github.pflooky.datacaterer.api.model.Task task() {
        return scalaDef.task();
    }

    public TaskBuilder name(String name) {
        return new TaskBuilder(scalaDef.name(name));
    }

    public TaskBuilder step(StepBuilder stepBuilder) {
        return new TaskBuilder(scalaDef.step(stepBuilder.step()));
    }

    public TaskBuilder steps(StepBuilder... stepBuilders) {
        return new TaskBuilder(scalaDef.steps(toScalaSeq(Arrays.stream(stepBuilders).map(StepBuilder::step).collect(Collectors.toList()))));
    }

}
