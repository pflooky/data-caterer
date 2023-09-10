package com.github.pflooky.datacaterer.api.java.model.config.connection;

import com.github.pflooky.datacaterer.api.java.SchemaBuilder;
import com.github.pflooky.datacaterer.api.java.model.CountBuilder;
import com.github.pflooky.datacaterer.api.java.model.FieldBuilder;
import com.github.pflooky.datacaterer.api.java.model.GeneratorBuilder;
import com.github.pflooky.datacaterer.api.java.model.StepBuilder;
import com.github.pflooky.datacaterer.api.java.model.TaskBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.ConnectionConfigWithTaskBuilder;

import java.util.Optional;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaOption;

public abstract class ConnectionTaskBuilder {
    private ConnectionConfigWithTaskBuilder connectionConfigWithTaskBuilder = new ConnectionConfigWithTaskBuilder();
    private Optional<TaskBuilder> optTask = Optional.empty();
    private Optional<StepBuilder> optStep = Optional.empty();

    public ConnectionTaskBuilder(com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder scalaDef) {
        setConnectionConfigWithTaskBuilder(new ConnectionConfigWithTaskBuilder(scalaDef.connectionConfigWithTaskBuilder()));
        var optTask = scalaDef.task().isDefined() ? Optional.of(new TaskBuilder(scalaDef.task().get())) : Optional.empty();
        setOptTask((Optional<TaskBuilder>) optTask);
        var optStep = scalaDef.step().isDefined() ? Optional.of(new StepBuilder(scalaDef.step().get())) : Optional.empty();
        setOptStep((Optional<StepBuilder>) optStep);
    }

    public com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder connectionTaskBuilder() {
        var scalaDef = new com.github.pflooky.datacaterer.api.connection.ConnectionTaskBuilder() {
        };
        scalaDef.apply(
                connectionConfigWithTaskBuilder.connectionConfig(),
                toScalaOption(optTask.map(TaskBuilder::task)),
                toScalaOption(optStep.map(StepBuilder::step))
        );
        return scalaDef;
    }

    public ConnectionConfigWithTaskBuilder getConnectionConfigWithTaskBuilder() {
        return connectionConfigWithTaskBuilder;
    }


    public void setConnectionConfigWithTaskBuilder(ConnectionConfigWithTaskBuilder connectionConfigWithTaskBuilder) {
        this.connectionConfigWithTaskBuilder = connectionConfigWithTaskBuilder;
    }

    public Optional<TaskBuilder> getOptTask() {
        return optTask;
    }

    public void setOptTask(Optional<TaskBuilder> optTask) {
        this.optTask = optTask;
    }

    public Optional<StepBuilder> getOptStep() {
        return optStep;
    }

    public void setOptStep(Optional<StepBuilder> optStep) {
        this.optStep = optStep;
    }

    public ConnectionTaskBuilder schema(FieldBuilder field, FieldBuilder... fields) {
        setOptStep(Optional.of(getStep().schema(field, fields)));
        return this;
    }

    public ConnectionTaskBuilder schema(SchemaBuilder schemaBuilder) {
        setOptStep(Optional.of(getStep().schema(schemaBuilder)));
        return this;
    }

    public ConnectionTaskBuilder count(CountBuilder countBuilder) {
        setOptStep(Optional.of(getStep().count(countBuilder)));
        return this;
    }

    public ConnectionTaskBuilder count(GeneratorBuilder generatorBuilder) {
        setOptStep(Optional.of(getStep().count(generatorBuilder)));
        return this;
    }

    public ConnectionTaskBuilder numPartitions(int numPartitions) {
        setOptStep(Optional.of(getStep().numPartitions(numPartitions)));
        return this;
    }

    public ConnectionTaskBuilder task(TaskBuilder taskBuilder) {
        setOptTask(Optional.of(taskBuilder));
        return this;
    }

    protected StepBuilder getStep() {
        return optStep.orElseGet(StepBuilder::new);
    }
}
