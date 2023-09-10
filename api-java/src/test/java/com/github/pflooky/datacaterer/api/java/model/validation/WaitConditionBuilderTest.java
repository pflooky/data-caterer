package com.github.pflooky.datacaterer.api.java.model.validation;

import com.github.pflooky.datacaterer.api.model.Constants;
import com.github.pflooky.datacaterer.api.model.DataExistsWaitCondition;
import com.github.pflooky.datacaterer.api.model.FileExistsWaitCondition;
import com.github.pflooky.datacaterer.api.model.PauseWaitCondition;
import com.github.pflooky.datacaterer.api.model.WebhookWaitCondition;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WaitConditionBuilderTest {

    @Test
    public void canCreateWaitConditionWithDefaults() {
        var result = new WaitConditionBuilder().waitCondition();

        assertTrue(result instanceof PauseWaitCondition);
        assertEquals(0, ((PauseWaitCondition) result).pauseInSeconds());
    }

    @Test
    public void canCreateWaitConditionWithPause() {
        var result = new WaitConditionBuilder().pause(2).waitCondition();

        assertTrue(result instanceof PauseWaitCondition);
        assertEquals(2, ((PauseWaitCondition) result).pauseInSeconds());
    }

    @Test
    public void canCreateWaitConditionWithFile() {
        var result = new WaitConditionBuilder().file("/my/generated/file").waitCondition();

        assertTrue(result instanceof FileExistsWaitCondition);
        assertEquals("/my/generated/file", ((FileExistsWaitCondition) result).path());
    }

    @Test
    public void canCreateWaitConditionWithDataExists() {
        var result = new WaitConditionBuilder()
                .dataExists(
                        "my_postgres",
                        Map.of(Constants.JDBC_TABLE(), "public.accounts"),
                        "year == 2023"
                )
                .waitCondition();

        assertTrue(result instanceof DataExistsWaitCondition);
        var dataExists = (DataExistsWaitCondition) result;
        assertEquals("my_postgres", dataExists.dataSourceName());
        assertTrue(dataExists.options().get(Constants.JDBC_TABLE()).contains("public.accounts"));
        assertEquals("year == 2023", dataExists.expr());
    }

    @Test
    public void canCreateWaitConditionWithWebhookDefaults() {
        var result = new WaitConditionBuilder()
                .webhook(
                        "my_http",
                        "http://localhost:8080/get"
                )
                .waitCondition();

        assertTrue(result instanceof WebhookWaitCondition);
        var webhook = (WebhookWaitCondition) result;
        assertEquals("my_http", webhook.dataSourceName());
        assertEquals("http://localhost:8080/get", webhook.url());
        assertEquals("GET", webhook.method());
        assertEquals(200, webhook.statusCode());
    }

    @Test
    public void canCreateWaitConditionWithWebhook() {
        var result = new WaitConditionBuilder()
                .webhook(
                        "my_http",
                        "http://localhost:8080/post",
                        "POST",
                        202
                )
                .waitCondition();

        assertTrue(result instanceof WebhookWaitCondition);
        var webhook = (WebhookWaitCondition) result;
        assertEquals("my_http", webhook.dataSourceName());
        assertEquals("http://localhost:8080/post", webhook.url());
        assertEquals("POST", webhook.method());
        assertEquals(202, webhook.statusCode());
    }

}