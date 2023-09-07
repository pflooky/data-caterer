package com.github.pflooky.datacaterer.api.java.model;

import org.junit.Test;

import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TasksBuilderTest {

    @Test
    public void canCreateTask() {
        var result = new TasksBuilder()
                .addTask("my_task", "my_json",
                        new StepBuilder()
                                .path("/my/data/path")
                ).tasks();

        assertEquals(1, result.tasks().length());
        var task = result.tasks().head();
        assertEquals("my_task", task.name());
        assertEquals(1, task.steps().length());
        assertTrue(task.steps().head().options().get("path").contains("/my/data/path"));
    }

    @Test
    public void canCreateMultipleTasks() {
        var result = new TasksBuilder()
                .addTasks("my_json",
                        new TaskBuilder().name("my_task"),
                        new TaskBuilder().name("my_second_task")
                ).tasks();

        assertEquals(2, result.tasks().length());
        assertTrue(result.tasks().exists(t -> Objects.equals(t.name(), "my_task")));
        assertTrue(result.tasks().exists(t -> Objects.equals(t.name(), "my_second_task")));
    }

}