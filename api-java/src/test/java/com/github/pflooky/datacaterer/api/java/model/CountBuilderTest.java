package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.model.Constants;
import org.junit.Test;

import static org.junit.Assert.*;

public class CountBuilderTest {

    @Test
    public void canCreateCountWithDefaults() {
        var result = new CountBuilder().count();

        assertTrue(result.total().contains(Constants.DEFAULT_COUNT_TOTAL()));
        assertTrue(result.perColumn().isEmpty());
        assertTrue(result.generator().isEmpty());
    }

    @Test
    public void canCreateCountWithTotal() {
        var result = new CountBuilder().total(10).count();

        assertTrue(result.total().contains(10L));
        assertTrue(result.perColumn().isEmpty());
        assertTrue(result.generator().isEmpty());
    }

    @Test
    public void canCreateCountWithPerColumn() {
        var result = new CountBuilder().perColumnTotal(100, "account_id").count();

        assertTrue(result.total().contains(Constants.DEFAULT_COUNT_TOTAL()));
        assertTrue(result.perColumn().isDefined());
        var perColRes = result.perColumn().get();
        assertEquals(1, perColRes.columnNames().length());
        assertTrue(perColRes.columnNames().contains("account_id"));
        assertTrue(perColRes.count().contains(100));
        assertTrue(result.generator().isEmpty());
    }

    @Test
    public void canCreateCountWithPerColumnWithDefault() {
        var result = new CountBuilder().columns("account_id", "year").count();

        assertTrue(result.total().contains(Constants.DEFAULT_COUNT_TOTAL()));
        assertTrue(result.perColumn().isDefined());
        var perColRes = result.perColumn().get();
        assertEquals(2, perColRes.columnNames().length());
        assertTrue(perColRes.columnNames().contains("account_id"));
        assertTrue(perColRes.columnNames().contains("year"));
        assertTrue(perColRes.count().contains(Constants.DEFAULT_PER_COLUMN_COUNT_TOTAL()));
        assertTrue(result.generator().isEmpty());
    }

    @Test
    public void canCreateCountWithPerColumnBuilder() {
        var result = new CountBuilder().perColumn(new PerColumnCountBuilder().columns("account_id")).count();

        assertTrue(result.total().contains(Constants.DEFAULT_COUNT_TOTAL()));
        assertTrue(result.perColumn().isDefined());
        var perColRes = result.perColumn().get();
        assertEquals(1, perColRes.columnNames().length());
        assertTrue(perColRes.columnNames().contains("account_id"));
        assertTrue(perColRes.count().contains(Constants.DEFAULT_PER_COLUMN_COUNT_TOTAL()));
        assertTrue(result.generator().isEmpty());
    }

    @Test
    public void canCreateCountWithPerColumnGenerator() {
        var result = new CountBuilder().perColumnGenerator(new GeneratorBuilder(), "account_id").count();

        assertTrue(result.total().contains(Constants.DEFAULT_COUNT_TOTAL()));
        assertTrue(result.perColumn().isDefined());
        var perColRes = result.perColumn().get();
        assertTrue(perColRes.generator().isDefined());
        assertEquals("random", perColRes.generator().get().type());
        assertTrue(perColRes.generator().get().options().isEmpty());
        assertEquals(1, perColRes.columnNames().size());
        assertTrue(perColRes.columnNames().contains("account_id"));
        assertTrue(perColRes.count().contains(Constants.DEFAULT_PER_COLUMN_COUNT_TOTAL()));
        assertTrue(result.generator().isEmpty());
    }

    @Test
    public void canCreateCountWithPerColumnGeneratorWithColumnTotal() {
        var result = new CountBuilder()
                .perColumnGeneratorWithTotal(100, new GeneratorBuilder(), "account_id")
                .count();

        assertTrue(result.total().contains(Constants.DEFAULT_COUNT_TOTAL()));
        assertTrue(result.perColumn().isDefined());
        var perColRes = result.perColumn().get();
        assertTrue(perColRes.generator().isDefined());
        assertEquals("random", perColRes.generator().get().type());
        assertTrue(perColRes.generator().get().options().isEmpty());
        assertEquals(1, perColRes.columnNames().size());
        assertTrue(perColRes.columnNames().contains("account_id"));
        assertTrue(perColRes.count().contains(100));
        assertTrue(result.generator().isEmpty());
    }

    @Test
    public void canCreateCountWithGenerator() {
        var result = new CountBuilder().generator(new GeneratorBuilder()).count();

        assertTrue(result.total().contains(Constants.DEFAULT_COUNT_TOTAL()));
        assertTrue(result.perColumn().isEmpty());
        assertTrue(result.generator().isDefined());
        var genRes = result.generator().get();
        assertEquals("random", genRes.type());
        assertTrue(genRes.options().isEmpty());
    }

}