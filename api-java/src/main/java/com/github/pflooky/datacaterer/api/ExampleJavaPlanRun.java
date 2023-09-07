package com.github.pflooky.datacaterer.api;

import com.github.pflooky.datacaterer.api.java.PlanRun;

import com.github.pflooky.datacaterer.api.java.model.TasksBuilder;
import scala.collection.immutable.List;

public class ExampleJavaPlanRun extends PlanRun {

    public ExampleJavaPlanRun() {
        setupPlanRun();
    }

    private void setupPlanRun() {
        var tasksBuilder = tasks().addTask("my_task", "minimal_json",
                step()
                        .path("app/src/test/resources/sample/json/minimal")
                        .schema(schema().addField(field().name("account_id")))
        );
        execute(tasksBuilder);
    }
}
