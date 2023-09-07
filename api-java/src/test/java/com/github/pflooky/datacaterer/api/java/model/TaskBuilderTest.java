package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.model.Constants;
import org.junit.Test;

import static org.junit.Assert.*;

public class TaskBuilderTest {

    @Test
    public void canCreateTaskWithDefaults() {
        var result = new TaskBuilder().task();

        assertEquals(Constants.DEFAULT_TASK_NAME(), result.name());
        assertTrue(result.steps().isEmpty());
    }

    @Test
    public void canCreateTaskWithStep() {
        var result = new TaskBuilder().name("my_task")
                .step(new StepBuilder().path("/my/json"))
                .task();

        assertEquals("my_task", result.name());
        assertEquals(1, result.steps().size());
        assertTrue(result.steps().head().options().contains(Constants.PATH()));
        assertTrue(result.steps().head().options().get(Constants.PATH()).contains("/my/json"));
    }

    @Test
    public void canCreateTaskWithSteps() {
        var result = new TaskBuilder().name("my_task")
                .steps(
                        new StepBuilder().path("/my/json"),
                        new StepBuilder().path("/my/csv")
                )
                .task();

        assertEquals("my_task", result.name());
        assertEquals(2, result.steps().size());
    }

}