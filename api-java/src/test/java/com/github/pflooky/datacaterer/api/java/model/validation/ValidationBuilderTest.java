package com.github.pflooky.datacaterer.api.java.model.validation;

import com.github.pflooky.datacaterer.api.model.ExpressionValidation;
import org.junit.Test;

import static org.junit.Assert.*;

public class ValidationBuilderTest {

    @Test
    public void canCreateValidationWithDefaults() {
        var result = new ValidationBuilder().validation();

        assertTrue(result instanceof ExpressionValidation);
        assertEquals("true", ((ExpressionValidation) result).expr());
        assertTrue(result.description().isEmpty());
        assertTrue(result.errorThreshold().isEmpty());
    }

    @Test
    public void canCreateValidationWithExpression() {
        var result = new ValidationBuilder()
                .description("check amount is valid")
                .expr("amount >= 0")
                .errorThreshold(0.1)
                .validation();

        assertTrue(result instanceof ExpressionValidation);
        assertEquals("amount >= 0", ((ExpressionValidation) result).expr());
        assertTrue(result.description().contains("check amount is valid"));
        assertTrue(result.errorThreshold().contains(0.1));
    }

    @Test
    public void canCreateValidationWithErrorThresholdOver1() {
        var result = new ValidationBuilder()
                .expr("amount >= 0")
                .errorThreshold(100)
                .validation();

        assertTrue(result instanceof ExpressionValidation);
        assertEquals("amount >= 0", ((ExpressionValidation) result).expr());
        assertTrue(result.errorThreshold().contains(100));
    }

}