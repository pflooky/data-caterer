package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.java.SchemaBuilder;
import com.github.pflooky.datacaterer.api.model.Constants;
import com.github.pflooky.datacaterer.api.model.IntegerType;
import com.github.pflooky.datacaterer.api.model.StringType;
import org.junit.Test;

import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.*;

public class StepBuilderTest {

    @Test
    public void canCreateStepWithDefaults() {
        var result = new StepBuilder().step();

        assertEquals(Constants.DEFAULT_STEP_NAME(), result.name());
        assertEquals(Constants.DEFAULT_STEP_TYPE(), result.type());
        assertTrue(result.count().total().contains(Constants.DEFAULT_COUNT_TOTAL()));
        assertTrue(result.count().generator().isEmpty());
        assertTrue(result.count().perColumn().isEmpty());
        assertTrue(result.options().isEmpty());
        assertTrue(result.schema().fields().isEmpty());
        assertTrue(result.enabled());
    }

    @Test
    public void canCreateStep() {
        var result = new StepBuilder().name("my_step").type("csv").enabled(false).step();

        assertEquals("my_step", result.name());
        assertEquals("csv", result.type());
        assertFalse(result.enabled());
    }

    @Test
    public void canCreateStepWithOption() {
        var result = new StepBuilder().option("my_key", "my_value").step();

        assertEquals(1, result.options().size());
        assertTrue(result.options().get("my_key").contains("my_value"));
    }

    @Test
    public void canCreateStepWithOptions() {
        var result = new StepBuilder().options(Map.of("my_key", "my_value")).step();

        assertEquals(1, result.options().size());
        assertTrue(result.options().get("my_key").contains("my_value"));
    }

    @Test
    public void canCreateStepWithJdbcTable() {
        var result = new StepBuilder().jdbcTable("public.my_table").step();

        assertEquals(1, result.options().size());
        assertTrue(result.options().get(Constants.JDBC_TABLE()).contains("public.my_table"));
    }

    @Test
    public void canCreateStepWithJdbcTableWithSchema() {
        var result = new StepBuilder().jdbcTable("public", "my_table").step();

        assertEquals(1, result.options().size());
        assertTrue(result.options().get(Constants.JDBC_TABLE()).contains("public.my_table"));
    }

    @Test
    public void canCreateStepWithCassandraTable() {
        var result = new StepBuilder().cassandraTable("account", "my_table").step();

        assertEquals(2, result.options().size());
        assertTrue(result.options().get(Constants.CASSANDRA_KEYSPACE()).contains("account"));
        assertTrue(result.options().get(Constants.CASSANDRA_TABLE()).contains("my_table"));
    }

    @Test
    public void canCreateStepWithJms() {
        var result = new StepBuilder().jmsDestination("/JNDI/Q/my_queue").step();

        assertEquals(1, result.options().size());
        assertTrue(result.options().get(Constants.JMS_DESTINATION_NAME()).contains("/JNDI/Q/my_queue"));
    }

    @Test
    public void canCreateStepWithKafka() {
        var result = new StepBuilder().kafkaTopic("my_topic").step();

        assertEquals(1, result.options().size());
        assertTrue(result.options().get(Constants.KAFKA_TOPIC()).contains("my_topic"));
    }

    @Test
    public void canCreateStepWithPath() {
        var result = new StepBuilder().path("/my/data/json").step();

        assertEquals(1, result.options().size());
        assertTrue(result.options().get(Constants.PATH()).contains("/my/data/json"));
    }

    @Test
    public void canCreateStepWithPartitionBy() {
        var result = new StepBuilder().partitionBy("account_id", "year").step();

        assertEquals(1, result.options().size());
        assertTrue(result.options().get(Constants.PARTITION_BY()).contains("account_id,year"));
    }

    @Test
    public void canCreateStepWithNumPartitions() {
        var result = new StepBuilder().numPartitions(2).step();

        assertEquals(1, result.options().size());
        assertTrue(result.options().get(Constants.PARTITIONS()).contains("2"));
    }

    @Test
    public void canCreateStepWithRowsPerSecond() {
        var result = new StepBuilder().rowsPerSecond(20).step();

        assertEquals(1, result.options().size());
        assertTrue(result.options().get(Constants.ROWS_PER_SECOND()).contains("20"));
    }

    @Test
    public void canCreateStepWithCount() {
        var result = new StepBuilder().count(200).step();

        assertTrue(result.count().total().contains(200));
        assertTrue(result.count().perColumn().isEmpty());
        assertTrue(result.count().generator().isEmpty());
    }

    @Test
    public void canCreateStepWithCountBuilder() {
        var result = new StepBuilder().count(new CountBuilder().total(200)).step();

        assertTrue(result.count().total().contains(200));
        assertTrue(result.count().perColumn().isEmpty());
        assertTrue(result.count().generator().isEmpty());
    }

    @Test
    public void canCreateStepWithCountGenerator() {
        var result = new StepBuilder().count(new GeneratorBuilder()).step();

        assertTrue(result.count().total().contains(Constants.DEFAULT_COUNT_TOTAL()));
        assertTrue(result.count().perColumn().isEmpty());
        assertTrue(result.count().generator().isDefined());
        assertEquals(Constants.DEFAULT_GENERATOR_TYPE(), result.count().generator().get().type());
        assertTrue(result.count().generator().get().options().isEmpty());
    }

    @Test
    public void canCreateStepWithPerColumnCount() {
        var result = new StepBuilder().count(new PerColumnCountBuilder()).step();

        assertTrue(result.count().total().contains(Constants.DEFAULT_COUNT_TOTAL()));
        assertTrue(result.count().perColumn().isDefined());
        assertTrue(result.count().perColumn().get().columnNames().isEmpty());
        assertTrue(result.count().perColumn().get().count().contains(Constants.DEFAULT_PER_COLUMN_COUNT_TOTAL()));
        assertTrue(result.count().perColumn().get().generator().isEmpty());
        assertTrue(result.count().generator().isEmpty());
    }

    @Test
    public void canCreateStepWithSchema() {
        var result = new StepBuilder()
                .schema(
                        new SchemaBuilder()
                                .addField("account_id", StringType.instance())
                                .addField("year", IntegerType.instance())
                )
                .step();

        assertTrue(result.schema().fields().isDefined());
        var fields = result.schema().fields().get();
        assertEquals(2, fields.size());
        assertTrue(fields.exists(f -> Objects.equals(f.name(), "account_id")));
        assertTrue(fields.exists(f -> Objects.equals(f.name(), "year")));
    }

}