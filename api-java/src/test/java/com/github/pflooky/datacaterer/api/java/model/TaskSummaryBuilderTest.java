package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.model.Constants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TaskSummaryBuilderTest {

    @Test
    public void canCreateTaskSummaryWithDefaults() {
        var result = new TaskSummaryBuilder().taskSummary();

        assertEquals(Constants.DEFAULT_TASK_NAME(), result.taskSummary().name());
        assertEquals(Constants.DEFAULT_DATA_SOURCE_NAME(), result.taskSummary().dataSourceName());
        assertTrue(result.taskSummary().enabled());
        assertTrue(result.task().isEmpty());
    }

    @Test
    public void canCreateTaskSummaryWithTask() {
        var result = new TaskSummaryBuilder()
                .dataSource("my_ds")
                .task(new TaskBuilder().name("my_task"))
                .enabled(false)
                .taskSummary();

        assertEquals("my_task", result.taskSummary().name());
        assertEquals("my_ds", result.taskSummary().dataSourceName());
        assertFalse(result.taskSummary().enabled());
        assertTrue(result.task().isDefined());
        assertEquals("my_task", result.task().get().name());
    }

    @Test
    public void canCreateTaskSummaryWithTaskOverridesName() {
        var result = new TaskSummaryBuilder()
                .name("first_name")
                .task(new TaskBuilder().name("my_task"))
                .taskSummary();

        assertEquals("my_task", result.taskSummary().name());
        assertTrue(result.task().isDefined());
        assertEquals("my_task", result.task().get().name());
    }

    @Test
    public void canCreateTaskSummaryWithTaskKeepsTaskName() {
        var result = new TaskSummaryBuilder()
                .task(new TaskBuilder().name("my_task"))
                .name("second_name")
                .taskSummary();

        assertEquals("my_task", result.taskSummary().name());
        assertTrue(result.task().isDefined());
        assertEquals("my_task", result.task().get().name());
    }

}