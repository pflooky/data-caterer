package com.github.pflooky.datacaterer.api.java.model.validation;

import com.github.pflooky.datacaterer.api.model.PauseWaitCondition;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataSourceValidationBuilderTest {

    @Test
    public void canCreateDataSourceValidationWithDefaults() {
        var result = new DataSourceValidationBuilder().dataSourceValidation();

        assertTrue(result.waitCondition() instanceof PauseWaitCondition);
        assertEquals(0, ((PauseWaitCondition) result.waitCondition()).pauseInSeconds());
        assertTrue(result.options().isEmpty());
        assertTrue(result.validations().isEmpty());
    }

    @Test
    public void canCreateDataSourceValidationWithOptions() {
        var result = new DataSourceValidationBuilder()
                .option("my_key", "my_value")
                .options(Map.of("more_key", "more_value"))
                .dataSourceValidation();

        assertEquals(2, result.options().size());
        assertTrue(result.options().get("my_key").contains("my_value"));
        assertTrue(result.options().get("more_key").contains("more_value"));
    }

    @Test
    public void canCreateDataSourceValidationWithValidation() {
        var result = new DataSourceValidationBuilder()
                .addValidation(new ValidationBuilder())
                .dataSourceValidation();

        assertEquals(1, result.validations().size());
    }

    @Test
    public void canCreateDataSourceValidationWithValidations() {
        var result = new DataSourceValidationBuilder()
                .addValidation(new ValidationBuilder())
                .validations(
                        new ValidationBuilder().expr("amount < 0"),
                        new ValidationBuilder().expr("name != 'Peter'").errorThreshold(1)
                )
                .dataSourceValidation();

        assertEquals(3, result.validations().size());
    }

    @Test
    public void canCreateDataSourceValidationWithWaitCondition() {
        var result = new DataSourceValidationBuilder()
                .wait(new WaitConditionBuilder().pause(2))
                .dataSourceValidation();

        assertTrue(result.waitCondition() instanceof PauseWaitCondition);
        assertEquals(2, ((PauseWaitCondition) result.waitCondition()).pauseInSeconds());
    }

}