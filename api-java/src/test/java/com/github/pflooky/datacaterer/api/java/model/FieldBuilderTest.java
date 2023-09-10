package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.java.SchemaBuilder;
import com.github.pflooky.datacaterer.api.model.Constants;
import com.github.pflooky.datacaterer.api.model.DoubleType;
import com.github.pflooky.datacaterer.api.model.StringType;
import org.junit.Test;

import java.sql.Date;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FieldBuilderTest {

    @Test
    public void canCreateFieldWithDefaults() {
        var result = new FieldBuilder().field();

        assertEquals("string", result.type().get());
        assertEquals("random", result.generator().get().type());
        assertTrue(result.generator().get().options().isEmpty());
        assertTrue(result.schema().isEmpty());
        assertTrue(result.nullable());
    }

    @Test
    public void canCreateFieldWithSchema() {
        var result = new FieldBuilder()
                .schema(new SchemaBuilder().addField("account_id", StringType.instance()))
                .field();

        assertEquals("string", result.type().get());
        assertEquals("random", result.generator().get().type());
        assertTrue(result.generator().get().options().isEmpty());
        assertTrue(result.schema().isDefined());
        var schema = result.schema().get();
        assertTrue(schema.fields().isDefined());
        assertEquals(1, schema.fields().get().size());
        assertTrue(schema.fields().get().exists(f -> Objects.equals(f.name(), "account_id")));
        assertTrue(result.nullable());
    }

    @Test
    public void canCreateFieldNotNullable() {
        var result = new FieldBuilder().nullable(false).field();

        assertEquals("string", result.type().get());
        assertFalse(result.nullable());
        assertTrue(result.generator().get().options().isEmpty());
    }

    @Test
    public void canCreateFieldGenerator() {
        var result = new FieldBuilder().generator(new GeneratorBuilder().sql("RAND() * 2")).field();

        assertEquals("string", result.type().get());
        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().get("sql").contains("RAND() * 2"));
    }

    @Test
    public void canCreateFieldRandom() {
        var result = new FieldBuilder().random().field();

        assertEquals("string", result.type().get());
        assertTrue(result.generator().get().options().isEmpty());
    }

    @Test
    public void canCreateFieldOverridingFieldType() {
        var result = new FieldBuilder().sql("true").random().field();

        assertEquals("string", result.type().get());
        assertTrue(result.generator().get().options().contains("sql"));
        assertTrue(result.generator().get().options().get("sql").contains("true"));
    }

    @Test
    public void canCreateFieldSql() {
        var result = new FieldBuilder().sql("CAST(RAND() * 2 AS STRING)").field();

        assertEquals("string", result.type().get());
        assertTrue(result.generator().get().options().contains("sql"));
        assertTrue(result.generator().get().options().get("sql").contains("CAST(RAND() * 2 AS STRING)"));
    }

    @Test
    public void canCreateFieldSqlWithType() {
        var result = new FieldBuilder().sql("RAND() * 2").type(DoubleType.instance()).field();

        assertEquals("double", result.type().get());
        assertTrue(result.generator().get().options().contains("sql"));
        assertTrue(result.generator().get().options().get("sql").contains("RAND() * 2"));
    }

    @Test
    public void canCreateFieldRegex() {
        var result = new FieldBuilder().regex("[a-z]{4}").field();

        assertEquals("string", result.type().get());
        assertTrue(result.generator().get().options().contains("regex"));
        assertTrue(result.generator().get().options().get("regex").contains("[a-z]{4}"));
    }

    @Test
    public void canCreateFieldOneOf() {
        var result = new FieldBuilder().oneOf("open", "closed").field();

        assertEquals("string", result.type().get());
        assertTrue(result.generator().get().options().contains("oneOf"));
        var resOneOf = result.generator().get().options().get("oneOf").get();
        var oneOfList = ((scala.collection.immutable.Stream) resOneOf).toList();
        assertEquals(2, oneOfList.size());
        assertTrue(oneOfList.contains("open"));
        assertTrue(oneOfList.contains("closed"));
    }

    @Test
    public void canCreateFieldOneOfDouble() {
        var result = new FieldBuilder().oneOf(20.1, 42.2).field();

        assertEquals("double", result.type().get());
        assertTrue(result.generator().get().options().contains("oneOf"));
        var resOneOf = result.generator().get().options().get("oneOf").get();
        var oneOfList = ((scala.collection.immutable.Stream) resOneOf).toList();
        assertEquals(2, oneOfList.size());
        assertTrue(oneOfList.contains(20.1));
        assertTrue(oneOfList.contains(42.2));
    }

    @Test
    public void canCreateFieldOneOfDate() {
        var date = Date.valueOf("2020-01-01");
        var result = new FieldBuilder()
                .oneOf(date)
                .field();

        assertTrue(result.generator().get().options().contains("oneOf"));
        var resOneOf = result.generator().get().options().get("oneOf").get();
        var oneOfList = ((scala.collection.immutable.Stream) resOneOf).toList();
        assertEquals(1, oneOfList.size());
        assertTrue(oneOfList.contains(date));
    }

    @Test
    public void canCreateFieldOptions() {
        Map<String, Object> options = Map.of("my_key", "my_value");
        var result = new FieldBuilder().options(options).field();

        assertEquals("string", result.type().get());
        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains("my_key"));
        assertTrue(result.generator().get().options().get("my_key").contains("my_value"));
    }

    @Test
    public void canCreateFieldOption() {
        var result = new FieldBuilder().option("my_key", "my_value").field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains("my_key"));
        assertTrue(result.generator().get().options().get("my_key").contains("my_value"));
    }

    @Test
    public void canCreateFieldSeed() {
        var result = new FieldBuilder().seed(1).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains("seed"));
        assertTrue(result.generator().get().options().get("seed").contains("1"));
    }

    @Test
    public void canCreateFieldEnableNull() {
        var result = new FieldBuilder().enableNull(true).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.ENABLED_NULL()));
        assertTrue(result.generator().get().options().get(Constants.ENABLED_NULL()).contains("true"));
    }

    @Test
    public void canCreateFieldNullProbability() {
        var result = new FieldBuilder().nullProbability(0.9).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.PROBABILITY_OF_NULL()));
        assertTrue(result.generator().get().options().get(Constants.PROBABILITY_OF_NULL()).contains("0.9"));
    }

    @Test
    public void canCreateFieldEnableEdgeCases() {
        var result = new FieldBuilder().enableEdgeCases(true).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.ENABLED_EDGE_CASE()));
        assertTrue(result.generator().get().options().get(Constants.ENABLED_EDGE_CASE()).contains("true"));
    }

    @Test
    public void canCreateFieldEdgeCaseProbability() {
        var result = new FieldBuilder().edgeCaseProbability(0.9).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.PROBABILITY_OF_EDGE_CASE()));
        assertTrue(result.generator().get().options().get(Constants.PROBABILITY_OF_EDGE_CASE()).contains("0.9"));
    }

    @Test
    public void canCreateFieldStaticValue() {
        var result = new FieldBuilder().staticValue(0).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.STATIC()));
        assertTrue(result.generator().get().options().get(Constants.STATIC()).contains("0"));
    }

    @Test
    public void canCreateFieldUnique() {
        var result = new FieldBuilder().unique(true).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.IS_UNIQUE()));
        assertTrue(result.generator().get().options().get(Constants.IS_UNIQUE()).contains("true"));
    }

    @Test
    public void canCreateFieldArrayType() {
        var result = new FieldBuilder().arrayType("string").field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.ARRAY_TYPE()));
        assertTrue(result.generator().get().options().get(Constants.ARRAY_TYPE()).contains("string"));
    }

    @Test
    public void canCreateFieldExpression() {
        var result = new FieldBuilder().expression("#{Name.name}").field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.EXPRESSION()));
        assertTrue(result.generator().get().options().get(Constants.EXPRESSION()).contains("#{Name.name}"));
    }

    @Test
    public void canCreateFieldAvgLength() {
        var result = new FieldBuilder().avgLength(3).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.AVERAGE_LENGTH()));
        assertTrue(result.generator().get().options().get(Constants.AVERAGE_LENGTH()).contains("3"));
    }

    @Test
    public void canCreateFieldMin() {
        var result = new FieldBuilder().min(1).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.MINIMUM()));
        assertTrue(result.generator().get().options().get(Constants.MINIMUM()).contains("1"));
    }

    @Test
    public void canCreateFieldMinLength() {
        var result = new FieldBuilder().minLength(1).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.MINIMUM_LENGTH()));
        assertTrue(result.generator().get().options().get(Constants.MINIMUM_LENGTH()).contains("1"));
    }

    @Test
    public void canCreateFieldArrayMinLength() {
        var result = new FieldBuilder().arrayMinLength(1).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.ARRAY_MINIMUM_LENGTH()));
        assertTrue(result.generator().get().options().get(Constants.ARRAY_MINIMUM_LENGTH()).contains("1"));
    }

    @Test
    public void canCreateFieldMax() {
        var result = new FieldBuilder().max(1).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.MAXIMUM()));
        assertTrue(result.generator().get().options().get(Constants.MAXIMUM()).contains("1"));
    }

    @Test
    public void canCreateFieldMaxLength() {
        var result = new FieldBuilder().maxLength(1).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.MAXIMUM_LENGTH()));
        assertTrue(result.generator().get().options().get(Constants.MAXIMUM_LENGTH()).contains("1"));
    }

    @Test
    public void canCreateFieldArrayMaxLength() {
        var result = new FieldBuilder().arrayMaxLength(1).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.ARRAY_MAXIMUM_LENGTH()));
        assertTrue(result.generator().get().options().get(Constants.ARRAY_MAXIMUM_LENGTH()).contains("1"));
    }

    @Test
    public void canCreateFieldNumericPrecision() {
        var result = new FieldBuilder().numericPrecision(4).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.NUMERIC_PRECISION()));
        assertTrue(result.generator().get().options().get(Constants.NUMERIC_PRECISION()).contains("4"));
    }

    @Test
    public void canCreateFieldNumericScale() {
        var result = new FieldBuilder().numericScale(3).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.NUMERIC_SCALE()));
        assertTrue(result.generator().get().options().get(Constants.NUMERIC_SCALE()).contains("3"));
    }

    @Test
    public void canCreateFieldOmit() {
        var result = new FieldBuilder().omit(true).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.OMIT()));
        assertTrue(result.generator().get().options().get(Constants.OMIT()).contains("true"));
    }

    @Test
    public void canCreateFieldPrimaryKey() {
        var result = new FieldBuilder().primaryKey(true).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.IS_PRIMARY_KEY()));
        assertTrue(result.generator().get().options().get(Constants.IS_PRIMARY_KEY()).contains("true"));
    }

    @Test
    public void canCreateFieldPrimaryKeyPosition() {
        var result = new FieldBuilder().primaryKeyPosition(1).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.PRIMARY_KEY_POSITION()));
        assertTrue(result.generator().get().options().get(Constants.PRIMARY_KEY_POSITION()).contains("1"));
    }

    @Test
    public void canCreateFieldClusteringKeyPosition() {
        var result = new FieldBuilder().clusteringPosition(2).field();

        assertEquals(1, result.generator().get().options().size());
        assertTrue(result.generator().get().options().contains(Constants.CLUSTERING_POSITION()));
        assertTrue(result.generator().get().options().get(Constants.CLUSTERING_POSITION()).contains("2"));
    }

}