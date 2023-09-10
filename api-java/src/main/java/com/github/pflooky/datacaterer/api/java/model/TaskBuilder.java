package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.model.Constants;
import com.github.pflooky.datacaterer.api.model.Task;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaList;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaSeq;

/**
 * A task can be seen as a representation of a data source.
 * A task can contain steps which represent sub data sources within it.<br>
 * For example, you can define a Postgres task for database 'customer' with steps to generate data for
 * tables 'public.account' and 'public.transactions' within it.
 */
public final class TaskBuilder {

    private final com.github.pflooky.datacaterer.api.TaskBuilder scalaDef;

    /**
     * Wrapper constructor for Scala TaskBuilder
     *
     * @param scalaDef Scala TaskBuilder
     */
    public TaskBuilder(com.github.pflooky.datacaterer.api.TaskBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    /**
     * Default constructor creates Task with no Steps
     */
    public TaskBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.TaskBuilder(
                new Task(Constants.DEFAULT_TASK_NAME(), toScalaList(Collections.emptyList()))
        );
    }

    /**
     * Scala task with all parameters
     *
     * @return Scala type of Task
     */
    public com.github.pflooky.datacaterer.api.model.Task task() {
        return scalaDef.task();
    }

    /**
     * Set name of the task
     *
     * @param name Name of the task
     * @return TaskBuilder
     */
    public TaskBuilder name(String name) {
        return new TaskBuilder(scalaDef.name(name));
    }

    /**
     * Define one or more steps for the task.
     * Steps can be seen as sub data sources (i.e. tables in a database, topics/queues in messaging systems)
     *
     * @param stepBuilder  Required step builder
     * @param stepBuilders Any number of step builders
     * @return TaskBuilder
     */
    public TaskBuilder steps(StepBuilder stepBuilder, StepBuilder... stepBuilders) {
        return new TaskBuilder(scalaDef.steps(
                stepBuilder.step(),
                toScalaSeq(Arrays.stream(stepBuilders).map(StepBuilder::step).collect(Collectors.toList()))
        ));
    }

}
