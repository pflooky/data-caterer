package com.github.pflooky.datacaterer.api.java;

import com.github.pflooky.datacaterer.api.java.model.FieldBuilder;
import com.github.pflooky.datacaterer.api.model.DoubleType;
import com.github.pflooky.datacaterer.api.model.IntegerType;
import com.github.pflooky.datacaterer.api.model.StringType;
import org.junit.Test;

import java.util.Objects;

import static org.junit.Assert.*;

public class SchemaBuilderTest {

    @Test
    public void canCreateSchemaWithSingleField() {
        var result = new SchemaBuilder()
                .addField("account_id", StringType.instance())
                .schema();

        assertTrue(result.fields().isDefined());
        assertEquals(1, result.fields().get().length());
        assertEquals("account_id", result.fields().get().head().name());
        assertTrue(result.fields().get().head().type().contains("string"));
    }

    @Test
    public void canCreateSchemaWithMultipleFields() {
        var result = new SchemaBuilder()
                .addField("account_id", StringType.instance())
                .addField("year", IntegerType.instance())
                .schema();

        assertTrue(result.fields().isDefined());
        assertEquals(2, result.fields().get().length());
        assertTrue(result.fields().get().exists(f -> Objects.equals(f.name(), "account_id")));
        assertTrue(result.fields().get().exists(f -> Objects.equals(f.name(), "year")));
    }

    @Test
    public void canCreateSchemaWithFieldBuilder() {
        var result = new SchemaBuilder()
                .addField(new FieldBuilder().name("account_id"))
                .addField(new FieldBuilder().name("year"))
                .schema();

        assertTrue(result.fields().isDefined());
        assertEquals(2, result.fields().get().length());
        assertTrue(result.fields().get().exists(f -> Objects.equals(f.name(), "account_id")));
        assertTrue(result.fields().get().exists(f -> Objects.equals(f.name(), "year")));
    }

    @Test
    public void canCreateSchemaWithAddFields() {
        var result = new SchemaBuilder()
                .addFields(
                        new FieldBuilder().name("account_id").type(StringType.instance()),
                        new FieldBuilder().name("year").type(IntegerType.instance())
                )
                .schema();

        assertTrue(result.fields().isDefined());
        assertEquals(2, result.fields().get().length());
        assertTrue(result.fields().get().exists(f -> Objects.equals(f.name(), "account_id")));
        assertTrue(result.fields().get().exists(f -> Objects.equals(f.name(), "year")));
    }

    @Test
    public void canCreateSchemaWithFieldsAddedFromAddFieldsAndAddField() {
        var result = new SchemaBuilder()
                .addFields(
                        new FieldBuilder().name("account_id").type(StringType.instance()),
                        new FieldBuilder().name("year").type(IntegerType.instance())
                )
                .addField(new FieldBuilder().name("name"))
                .addField("amount", DoubleType.instance())
                .schema();

        assertTrue(result.fields().isDefined());
        assertEquals(4, result.fields().get().length());
        assertTrue(result.fields().get().exists(f -> Objects.equals(f.name(), "account_id")));
        assertTrue(result.fields().get().exists(f -> Objects.equals(f.name(), "year")));
        assertTrue(result.fields().get().exists(f -> Objects.equals(f.name(), "name")));
        assertTrue(result.fields().get().exists(f -> Objects.equals(f.name(), "amount")));
    }

}