package com.github.pflooky.datacaterer.api.java;

import com.github.pflooky.datacaterer.api.java.model.SinkOptionsBuilder;
import com.github.pflooky.datacaterer.api.java.model.TaskBuilder;
import com.github.pflooky.datacaterer.api.java.model.TaskSummaryBuilder;
import com.github.pflooky.datacaterer.api.model.ForeignKeyRelation;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PlanBuilderTest {

    @Test
    public void canCreatePlanWithDefaults() {
        var result = new PlanBuilder().plan().plan();

        assertFalse(result.name().isEmpty());
        assertFalse(result.description().isEmpty());
        assertTrue(result.sinkOptions().isEmpty());
        assertTrue(result.tasks().isEmpty());
        assertTrue(result.validations().isEmpty());
    }

    @Test
    public void canCreatePlanWithTask() {
        var result = new PlanBuilder()
                .name("my_plan")
                .description("Big data plan")
                .seed(10)
                .locale("in")
                .taskSummaries(
                        new TaskSummaryBuilder()
                                .dataSource("my_json")
                                .task(new TaskBuilder())
                ).plan().plan();

        assertEquals("my_plan", result.name());
        assertEquals("Big data plan", result.description());
        assertEquals(1, result.tasks().size());
        assertTrue(result.sinkOptions().isDefined());
        assertTrue(result.sinkOptions().get().seed().contains("10"));
        assertTrue(result.sinkOptions().get().locale().contains("in"));
    }

    @Test
    public void canCreatePlanWithSinkOptions() {
        var result = new PlanBuilder()
                .sinkOptions(new SinkOptionsBuilder().seed(10))
                .plan().plan();

        assertTrue(result.sinkOptions().isDefined());
        assertTrue(result.sinkOptions().get().seed().contains("10"));
        assertTrue(result.sinkOptions().get().locale().isEmpty());
    }

    @Test
    public void canCreatePlanWithForeignKeys() {
        var result = new PlanBuilder()
                .addForeignKeyRelationship(
                        new ForeignKeyRelation("my_json", "my_step", "acc_id"),
                        new ForeignKeyRelation("my_csv", "csv_step", "acc_id"),
                        new ForeignKeyRelation("my_orc", "orc_step", "account_id")
                ).plan().plan();

        assertTrue(result.sinkOptions().isDefined());
        assertEquals(1, result.sinkOptions().get().foreignKeys().size());
        assertTrue(result.sinkOptions().get().foreignKeys().contains("my_json.my_step.acc_id"));
        var fk = result.sinkOptions().get().foreignKeys().get("my_json.my_step.acc_id").get();
        assertTrue(fk.contains("my_csv.csv_step.acc_id"));
        assertTrue(fk.contains("my_orc.orc_step.account_id"));
    }

    @Test
    public void canCreatePlanWithForeignKeysWithList() {
        var result = new PlanBuilder()
                .addForeignKeyRelationship(
                        new ForeignKeyRelation("my_json", "my_step", "acc_id"),
                        new ForeignKeyRelation("my_csv", "csv_step", "acc_id")
                )
                .addForeignKeyRelationship(
                        new ForeignKeyRelation("my_postgres", "postgres_step", "cust_num"),
                        List.of(new ForeignKeyRelation("my_cassandra", "cassandra_step", "acc_id"))
                )
                .plan().plan();

        assertTrue(result.sinkOptions().isDefined());
        assertEquals(2, result.sinkOptions().get().foreignKeys().size());
        assertTrue(result.sinkOptions().get().foreignKeys().contains("my_json.my_step.acc_id"));
        var fk = result.sinkOptions().get().foreignKeys().get("my_json.my_step.acc_id").get();
        assertTrue(fk.contains("my_csv.csv_step.acc_id"));
        var fk2 = result.sinkOptions().get().foreignKeys().get("my_postgres.postgres_step.cust_num").get();
        assertTrue(fk2.contains("my_cassandra.cassandra_step.acc_id"));
    }

}