package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.model.Constants;
import org.junit.Test;

import static org.junit.Assert.*;

public class PerColumnCountBuilderTest {

    @Test
    public void canCreatePerColumnCountWithDefaults() {
        var result = new PerColumnCountBuilder().perColumnCount();

        assertTrue(result.count().contains(Constants.DEFAULT_PER_COLUMN_COUNT_TOTAL()));
        assertTrue(result.columnNames().isEmpty());
        assertTrue(result.generator().isEmpty());
    }

    @Test
    public void canCreatePerColumnCountWithColumns() {
        var result = new PerColumnCountBuilder().columns("account_id", "year").perColumnCount();

        assertTrue(result.count().contains(Constants.DEFAULT_PER_COLUMN_COUNT_TOTAL()));
        assertEquals(2, result.columnNames().size());
        assertTrue(result.columnNames().contains("account_id"));
        assertTrue(result.columnNames().contains("year"));
        assertTrue(result.generator().isEmpty());
    }

    @Test
    public void canCreatePerColumnCountWithTotal() {
        var result = new PerColumnCountBuilder().total(100, "account_id", "year").perColumnCount();

        assertTrue(result.count().contains(100));
        assertEquals(2, result.columnNames().size());
        assertTrue(result.columnNames().contains("account_id"));
        assertTrue(result.columnNames().contains("year"));
        assertTrue(result.generator().isEmpty());
    }

    @Test
    public void canCreatePerColumnCountWithGenerator() {
        var result = new PerColumnCountBuilder().generator(new GeneratorBuilder(), "account_id", "year").perColumnCount();

        assertTrue(result.count().contains(Constants.DEFAULT_PER_COLUMN_COUNT_TOTAL()));
        assertEquals(2, result.columnNames().size());
        assertTrue(result.columnNames().contains("account_id"));
        assertTrue(result.columnNames().contains("year"));
        assertTrue(result.generator().isDefined());
        assertEquals("random", result.generator().get().type());
        assertTrue(result.generator().get().options().isEmpty());
    }
}