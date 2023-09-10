package com.github.pflooky.datacaterer.api.java.model;

import com.github.pflooky.datacaterer.api.model.Constants;
import org.junit.Test;

import java.sql.Date;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GeneratorBuilderTest {

    @Test
    public void canCreateGeneratorWithDefaults() {
        var result = new GeneratorBuilder().generator();

        assertEquals("random", result.type());
        assertTrue(result.options().isEmpty());
    }

    @Test
    public void canCreateGeneratorRandom() {
        var result = new GeneratorBuilder().random().generator();

        assertEquals("random", result.type());
        assertTrue(result.options().isEmpty());
    }

    @Test
    public void canCreateGeneratorOverridingGeneratorType() {
        var result = new GeneratorBuilder().sql("true").random().generator();

        assertEquals("random", result.type());
        assertTrue(result.options().contains("sql"));
        assertTrue(result.options().get("sql").contains("true"));
    }

    @Test
    public void canCreateGeneratorSql() {
        var result = new GeneratorBuilder().sql("RAND() * 2").generator();

        assertEquals("sql", result.type());
        assertTrue(result.options().contains("sql"));
        assertTrue(result.options().get("sql").contains("RAND() * 2"));
    }

    @Test
    public void canCreateGeneratorRegex() {
        var result = new GeneratorBuilder().regex("[a-z]{4}").generator();

        assertEquals("regex", result.type());
        assertTrue(result.options().contains("regex"));
        assertTrue(result.options().get("regex").contains("[a-z]{4}"));
    }

    @Test
    public void canCreateGeneratorOneOf() {
        var result = new GeneratorBuilder().oneOf("open", "closed").generator();

        assertEquals("oneOf", result.type());
        assertTrue(result.options().contains("oneOf"));
        var resOneOf = result.options().get("oneOf").get();
        var oneOfList = ((scala.collection.immutable.Stream) resOneOf).toList();
        assertEquals(2, oneOfList.size());
        assertTrue(oneOfList.contains("open"));
        assertTrue(oneOfList.contains("closed"));
    }

    @Test
    public void canCreateGeneratorOneOfDouble() {
        var result = new GeneratorBuilder().oneOf(20.1, 42.2).generator();

        assertEquals("oneOf", result.type());
        assertTrue(result.options().contains("oneOf"));
        var resOneOf = result.options().get("oneOf").get();
        var oneOfList = ((scala.collection.immutable.Stream) resOneOf).toList();
        assertEquals(2, oneOfList.size());
        assertTrue(oneOfList.contains(20.1));
        assertTrue(oneOfList.contains(42.2));
    }

    @Test
    public void canCreateGeneratorOneOfDate() {
        var date = Date.valueOf("2020-01-01");
        var result = new GeneratorBuilder()
                .oneOf(date)
                .generator();

        assertEquals("oneOf", result.type());
        assertTrue(result.options().contains("oneOf"));
        var resOneOf = result.options().get("oneOf").get();
        var oneOfList = ((scala.collection.immutable.Stream) resOneOf).toList();
        assertEquals(1, oneOfList.size());
        assertTrue(oneOfList.contains(date));
    }

    @Test
    public void canCreateGeneratorOptions() {
        Map<String, Object> options = Map.of("my_key", "my_value");
        var result = new GeneratorBuilder().options(options).generator();

        assertEquals("random", result.type());
        assertEquals(1, result.options().size());
        assertTrue(result.options().contains("my_key"));
        assertTrue(result.options().get("my_key").contains("my_value"));
    }

    @Test
    public void canCreateGeneratorOption() {
        var result = new GeneratorBuilder().option("my_key", "my_value").generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains("my_key"));
        assertTrue(result.options().get("my_key").contains("my_value"));
    }

    @Test
    public void canCreateGeneratorSeed() {
        var result = new GeneratorBuilder().seed(1).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains("seed"));
        assertTrue(result.options().get("seed").contains("1"));
    }

    @Test
    public void canCreateGeneratorEnableNull() {
        var result = new GeneratorBuilder().enableNull(true).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.ENABLED_NULL()));
        assertTrue(result.options().get(Constants.ENABLED_NULL()).contains("true"));
    }

    @Test
    public void canCreateGeneratorNullProbability() {
        var result = new GeneratorBuilder().nullProbability(0.9).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.PROBABILITY_OF_NULL()));
        assertTrue(result.options().get(Constants.PROBABILITY_OF_NULL()).contains("0.9"));
    }

    @Test
    public void canCreateGeneratorEnableEdgeCases() {
        var result = new GeneratorBuilder().enableEdgeCases(true).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.ENABLED_EDGE_CASE()));
        assertTrue(result.options().get(Constants.ENABLED_EDGE_CASE()).contains("true"));
    }

    @Test
    public void canCreateGeneratorEdgeCaseProbability() {
        var result = new GeneratorBuilder().edgeCaseProbability(0.9).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.PROBABILITY_OF_EDGE_CASE()));
        assertTrue(result.options().get(Constants.PROBABILITY_OF_EDGE_CASE()).contains("0.9"));
    }

    @Test
    public void canCreateGeneratorStaticValue() {
        var result = new GeneratorBuilder().staticValue(0).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.STATIC()));
        assertTrue(result.options().get(Constants.STATIC()).contains("0"));
    }

    @Test
    public void canCreateGeneratorUnique() {
        var result = new GeneratorBuilder().unique(true).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.IS_UNIQUE()));
        assertTrue(result.options().get(Constants.IS_UNIQUE()).contains("true"));
    }

    @Test
    public void canCreateGeneratorArrayType() {
        var result = new GeneratorBuilder().arrayType("string").generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.ARRAY_TYPE()));
        assertTrue(result.options().get(Constants.ARRAY_TYPE()).contains("string"));
    }

    @Test
    public void canCreateGeneratorExpression() {
        var result = new GeneratorBuilder().expression("#{Name.name}").generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.EXPRESSION()));
        assertTrue(result.options().get(Constants.EXPRESSION()).contains("#{Name.name}"));
    }

    @Test
    public void canCreateGeneratorAvgLength() {
        var result = new GeneratorBuilder().avgLength(3).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.AVERAGE_LENGTH()));
        assertTrue(result.options().get(Constants.AVERAGE_LENGTH()).contains("3"));
    }

    @Test
    public void canCreateGeneratorMin() {
        var result = new GeneratorBuilder().min(1).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.MINIMUM()));
        assertTrue(result.options().get(Constants.MINIMUM()).contains("1"));
    }

    @Test
    public void canCreateGeneratorMinLength() {
        var result = new GeneratorBuilder().minLength(1).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.MINIMUM_LENGTH()));
        assertTrue(result.options().get(Constants.MINIMUM_LENGTH()).contains("1"));
    }

    @Test
    public void canCreateGeneratorArrayMinLength() {
        var result = new GeneratorBuilder().arrayMinLength(1).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.ARRAY_MINIMUM_LENGTH()));
        assertTrue(result.options().get(Constants.ARRAY_MINIMUM_LENGTH()).contains("1"));
    }

    @Test
    public void canCreateGeneratorMax() {
        var result = new GeneratorBuilder().max(1).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.MAXIMUM()));
        assertTrue(result.options().get(Constants.MAXIMUM()).contains("1"));
    }

    @Test
    public void canCreateGeneratorMaxLength() {
        var result = new GeneratorBuilder().maxLength(1).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.MAXIMUM_LENGTH()));
        assertTrue(result.options().get(Constants.MAXIMUM_LENGTH()).contains("1"));
    }

    @Test
    public void canCreateGeneratorArrayMaxLength() {
        var result = new GeneratorBuilder().arrayMaxLength(1).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.ARRAY_MAXIMUM_LENGTH()));
        assertTrue(result.options().get(Constants.ARRAY_MAXIMUM_LENGTH()).contains("1"));
    }

    @Test
    public void canCreateGeneratorNumericPrecision() {
        var result = new GeneratorBuilder().numericPrecision(4).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.NUMERIC_PRECISION()));
        assertTrue(result.options().get(Constants.NUMERIC_PRECISION()).contains("4"));
    }

    @Test
    public void canCreateGeneratorNumericScale() {
        var result = new GeneratorBuilder().numericScale(3).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.NUMERIC_SCALE()));
        assertTrue(result.options().get(Constants.NUMERIC_SCALE()).contains("3"));
    }

    @Test
    public void canCreateGeneratorOmit() {
        var result = new GeneratorBuilder().omit(true).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.OMIT()));
        assertTrue(result.options().get(Constants.OMIT()).contains("true"));
    }

    @Test
    public void canCreateGeneratorPrimaryKey() {
        var result = new GeneratorBuilder().primaryKey(true).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.IS_PRIMARY_KEY()));
        assertTrue(result.options().get(Constants.IS_PRIMARY_KEY()).contains("true"));
    }

    @Test
    public void canCreateGeneratorPrimaryKeyPosition() {
        var result = new GeneratorBuilder().primaryKeyPosition(1).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.PRIMARY_KEY_POSITION()));
        assertTrue(result.options().get(Constants.PRIMARY_KEY_POSITION()).contains("1"));
    }

    @Test
    public void canCreateGeneratorClusteringKeyPosition() {
        var result = new GeneratorBuilder().clusteringPosition(2).generator();

        assertEquals(1, result.options().size());
        assertTrue(result.options().contains(Constants.CLUSTERING_POSITION()));
        assertTrue(result.options().get(Constants.CLUSTERING_POSITION()).contains("2"));
    }

}