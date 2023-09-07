package com.github.pflooky.datacaterer.api.java;


import com.github.pflooky.datacaterer.api.java.model.CountBuilder;
import com.github.pflooky.datacaterer.api.java.model.FieldBuilder;
import com.github.pflooky.datacaterer.api.java.model.GeneratorBuilder;
import com.github.pflooky.datacaterer.api.java.model.StepBuilder;
import com.github.pflooky.datacaterer.api.java.model.TaskBuilder;
import com.github.pflooky.datacaterer.api.java.model.TaskSummaryBuilder;
import com.github.pflooky.datacaterer.api.java.model.TasksBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.DataCatererConfigurationBuilder;
import com.github.pflooky.datacaterer.api.java.model.validation.DataSourceValidationBuilder;
import com.github.pflooky.datacaterer.api.java.model.validation.ValidationConfigurationBuilder;
import com.github.pflooky.datacaterer.api.model.DataCatererConfiguration;
import com.github.pflooky.datacaterer.api.model.ForeignKeyRelation;
import com.github.pflooky.datacaterer.api.model.Task;
import com.github.pflooky.datacaterer.api.model.ValidationConfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaList;

public abstract class PlanRun {

    private com.github.pflooky.datacaterer.api.PlanRun basePlanRun;
    private PlanRun plan;
    private List<Task> tasks;
    private DataCatererConfiguration configuration;
    private List<ValidationConfiguration> validations;

    public com.github.pflooky.datacaterer.api.PlanRun getPlan() {
        return basePlanRun;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public DataCatererConfiguration getConfiguration() {
        return configuration;
    }

    public List<ValidationConfiguration> getValidations() {
        return validations;
    }

    public PlanBuilder plan() {
        return new PlanBuilder();
    }

    public TaskSummaryBuilder taskSummary() {
        return new TaskSummaryBuilder();
    }

    public TasksBuilder tasks() {
        return new TasksBuilder();
    }

    public TaskBuilder task() {
        return new TaskBuilder();
    }

    public StepBuilder step() {
        return new StepBuilder();
    }

    public SchemaBuilder schema() {
        return new SchemaBuilder();
    }

    public FieldBuilder field() {
        return new FieldBuilder();
    }

    public GeneratorBuilder generator() {
        return new GeneratorBuilder();
    }

    public CountBuilder count() {
        return new CountBuilder();
    }

    public DataCatererConfigurationBuilder configuration() {
        return new DataCatererConfigurationBuilder();
    }

    public DataSourceValidationBuilder dataSourceValidation() {
        return new DataSourceValidationBuilder();
    }

    public ValidationConfigurationBuilder validationConfig() {
        return new ValidationConfigurationBuilder();
    }

    public ForeignKeyRelation foreignField(String dataSource, String step, String column) {
        return new ForeignKeyRelation(dataSource, step, column);
    }

    public void execute(TasksBuilder tasks) {
        execute(List.of(tasks), plan(), configuration(), Collections.emptyList());
    }

    public void execute(PlanBuilder plan, DataCatererConfigurationBuilder configuration) {
        execute(Collections.emptyList(), plan, configuration, Collections.emptyList());
    }

    public void execute(
            List<TasksBuilder> tasks,
            PlanBuilder plan,
            DataCatererConfigurationBuilder configuration,
            List<ValidationConfigurationBuilder> validations
    ) {
        var basePlanRun = new com.github.pflooky.datacaterer.api.BasePlanRun();
        var mappedTasks = tasks.stream().map(TasksBuilder::tasks).collect(Collectors.toList());
        var mappedValidations = validations.stream().map(ValidationConfigurationBuilder::configuration).collect(Collectors.toList());
        basePlanRun.execute(
                toScalaList(mappedTasks),
                plan.plan(),
                configuration.configuration(),
                toScalaList(mappedValidations)
        );

        var javaTaskList = new ArrayList<Task>();
        var javaValidationList = new ArrayList<ValidationConfiguration>();
        basePlanRun._tasks().foreach(javaTaskList::add);
        basePlanRun._validations().foreach(javaValidationList::add);

        this.basePlanRun = basePlanRun;
    }

}
