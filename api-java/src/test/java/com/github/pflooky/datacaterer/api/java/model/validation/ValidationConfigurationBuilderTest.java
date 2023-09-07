package com.github.pflooky.datacaterer.api.java.model.validation;

import com.github.pflooky.datacaterer.api.model.Constants;
import com.github.pflooky.datacaterer.api.model.ExpressionValidation;
import com.github.pflooky.datacaterer.api.model.PauseWaitCondition;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ValidationConfigurationBuilderTest {

    @Test
    public void canCreateValidationConfigurationWithDefaults() {
        var result = new ValidationConfigurationBuilder().configuration().validationConfiguration();

        assertEquals(Constants.DEFAULT_VALIDATION_CONFIG_NAME(), result.name());
        assertEquals(Constants.DEFAULT_VALIDATION_DESCRIPTION(), result.description());
        assertTrue(result.dataSources().isEmpty());
    }

    @Test
    public void canCreateValidationConfigurationWithDataSourceValidation() {
        var result = new ValidationConfigurationBuilder()
                .name("my json validations")
                .description("Need to validate data")
                .addDataSourceValidation("my_json", new DataSourceValidationBuilder())
                .configuration().validationConfiguration();

        assertEquals("my json validations", result.name());
        assertEquals("Need to validate data", result.description());
        assertEquals(1, result.dataSources().size());
        assertTrue(result.dataSources().contains("my_json"));
    }

    @Test
    public void canCreateValidationConfigurationWithValidation() {
        var result = new ValidationConfigurationBuilder()
                .addValidations(
                        "my_json",
                        Map.of(Constants.PATH(), "/my/json/path"),
                        new ValidationBuilder()
                )
                .configuration().validationConfiguration();

        assertEquals(1, result.dataSources().size());
        assertTrue(result.dataSources().contains("my_json"));
        var myJsonValid = result.dataSources().get("my_json").get();
        assertEquals(1, myJsonValid.options().size());
        assertTrue(myJsonValid.options().get(Constants.PATH()).contains("/my/json/path"));
        assertEquals(1, myJsonValid.validations().size());
        assertTrue(myJsonValid.validations().head() instanceof ExpressionValidation);
    }

    @Test
    public void canCreateValidationConfigurationWithMultipleValidations() {
        var result = new ValidationConfigurationBuilder()
                .addValidations(
                        "my_json",
                        Map.of(Constants.PATH(), "/my/json/path"),
                        new ValidationBuilder(),
                        new ValidationBuilder().errorThreshold(200).expr("age > 10")
                )
                .configuration().validationConfiguration();

        assertEquals(1, result.dataSources().size());
        assertTrue(result.dataSources().contains("my_json"));
        var myJsonValid = result.dataSources().get("my_json").get();
        assertEquals(1, myJsonValid.options().size());
        assertTrue(myJsonValid.options().get(Constants.PATH()).contains("/my/json/path"));
        assertEquals(2, myJsonValid.validations().size());
        assertTrue(myJsonValid.validations().forall(v -> v instanceof ExpressionValidation));
    }

    @Test
    public void canCreateValidationConfigurationWithValidationAndWaitCondition() {
        var result = new ValidationConfigurationBuilder()
                .addValidations(
                        "my_json",
                        Map.of(Constants.PATH(), "/my/json/path"),
                        new WaitConditionBuilder().pause(2),
                        new ValidationBuilder()
                )
                .configuration().validationConfiguration();

        assertEquals(1, result.dataSources().size());
        assertTrue(result.dataSources().contains("my_json"));
        var myJsonValid = result.dataSources().get("my_json").get();
        assertEquals(1, myJsonValid.options().size());
        assertTrue(myJsonValid.options().get(Constants.PATH()).contains("/my/json/path"));
        assertEquals(1, myJsonValid.validations().size());
        assertTrue(myJsonValid.validations().head() instanceof ExpressionValidation);
        assertTrue(myJsonValid.waitCondition() instanceof PauseWaitCondition);
        assertEquals(2, ((PauseWaitCondition) myJsonValid.waitCondition()).pauseInSeconds());
    }

}