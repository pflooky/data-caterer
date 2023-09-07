package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.model.ForeignKeyRelation;
import org.junit.Test;

import java.util.List;
import java.util.Objects;

import static org.junit.Assert.*;

public class SinkOptionsBuilderTest {

    @Test
    public void canCreateSinkOptionsWithDefaults() {
        var result = new SinkOptionsBuilder().sinkOptions();

        assertTrue(result.foreignKeys().isEmpty());
        assertTrue(result.seed().isEmpty());
        assertTrue(result.locale().isEmpty());
    }

    @Test
    public void canCreateSinkOptionsWithSeed() {
        var result = new SinkOptionsBuilder().seed(10).sinkOptions();

        assertTrue(result.seed().contains("10"));
    }

    @Test
    public void canCreateSinkOptionsWithLocale() {
        var result = new SinkOptionsBuilder().locale("in").sinkOptions();

        assertTrue(result.locale().contains("in"));
    }

    @Test
    public void canCreateSinkOptionsWithForeignKey() {
        var result = new SinkOptionsBuilder()
                .foreignKey(
                        new ForeignKeyRelation("my_json", "my_step", "abc"),
                        new ForeignKeyRelation("my_csv", "csv_step", "account_id")
                )
                .sinkOptions();

        assertEquals(1, result.foreignKeys().size());
        var fk = result.foreignKeys().head();
        assertEquals("my_json.my_step.abc", fk._1);
        assertEquals(1, fk._2.size());
        assertEquals("my_csv.csv_step.account_id", fk._2.head());
    }

    @Test
    public void canCreateSinkOptionsWithForeignKeyList() {
        var result = new SinkOptionsBuilder()
                .foreignKey(
                        new ForeignKeyRelation("my_json", "my_step", "abc"),
                        List.of(new ForeignKeyRelation("my_csv", "csv_step", "account_id"))
                )
                .sinkOptions();

        assertEquals(1, result.foreignKeys().size());
        var fk = result.foreignKeys().head();
        assertEquals("my_json.my_step.abc", fk._1);
        assertEquals(1, fk._2.size());
        assertEquals("my_csv.csv_step.account_id", fk._2.head());
    }

    @Test
    public void canCreateSinkOptionsAddsForeignKeys() {
        var result = new SinkOptionsBuilder()
                .foreignKey(
                        new ForeignKeyRelation("my_json", "my_step", "abc"),
                        List.of(new ForeignKeyRelation("my_csv", "csv_step", "account_id"))
                )
                .foreignKey(
                        new ForeignKeyRelation("my_postgres", "postgres_step", "customer_id"),
                        new ForeignKeyRelation("my_cassandra", "cassandra_step", "customer_id"),
                        new ForeignKeyRelation("my_kafka", "kafka_step", "cust_id")
                )
                .sinkOptions();

        assertEquals(2, result.foreignKeys().size());
        var fkJson = result.foreignKeys().filter(x -> Objects.equals(x._1, "my_json.my_step.abc")).head();
        assertEquals(1, fkJson._2.size());
        assertEquals("my_csv.csv_step.account_id", fkJson._2.head());
        var fkPostgres = result.foreignKeys().filter(x -> Objects.equals(x._1, "my_postgres.postgres_step.customer_id")).head();
        assertEquals(2, fkPostgres._2.size());
        assertTrue(fkPostgres._2.contains("my_cassandra.cassandra_step.customer_id"));
        assertTrue(fkPostgres._2.contains("my_kafka.kafka_step.cust_id"));
    }

}