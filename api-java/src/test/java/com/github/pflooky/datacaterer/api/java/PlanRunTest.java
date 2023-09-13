package com.github.pflooky.datacaterer.api.java;

import com.github.pflooky.datacaterer.api.java.model.FieldBuilder;
import com.github.pflooky.datacaterer.api.java.model.config.connection.ConnectionTaskBuilder;
import com.github.pflooky.datacaterer.api.model.Constants;
import org.junit.Test;

import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PlanRunTest {

    private class BasePlanRun extends PlanRun {}

    @Test
    public void canCreatePlanRunWithSecondTaskInheritConfigFromFirstTask() {
        var baseResult = new BasePlanRun();
        ConnectionTaskBuilder myPostgres = baseResult.postgres("my_postgres", "jdbc:url")
                .table("account.accounts")
                .schema(new FieldBuilder().name("account_id"));
        ConnectionTaskBuilder mySecondPostgres = baseResult.postgres(myPostgres)
                .table("account.transactions")
                .schema(new FieldBuilder().name("txn_id"));

        baseResult.execute(myPostgres, mySecondPostgres);
        var result = baseResult.getPlan();

        assertEquals(2, result._tasks().size());
        var firstStep = result._tasks().head().steps().head();
        var secondStep = result._tasks().last().steps().head();
        var accountStep = firstStep.options().get(Constants.JDBC_TABLE()).contains("account.accounts") ? firstStep : secondStep;
        var txnStep  = firstStep.options().get(Constants.JDBC_TABLE()).contains("account.transactions") ? firstStep : secondStep;
        var accountSchema = accountStep.schema();
        var txnSchema = txnStep.schema();
        assertTrue(accountSchema.fields().isDefined());
        assertEquals(1, accountSchema.fields().get().size());
        assertTrue(accountSchema.fields().get().exists(f -> Objects.equals(f.name(), "account_id")));
        assertTrue(txnSchema.fields().isDefined());
        assertEquals(1, txnSchema.fields().get().size());
        assertTrue(txnSchema.fields().get().exists(f -> Objects.equals(f.name(), "txn_id")));

        assertEquals(1, result._configuration().connectionConfigByName().size());
        assertTrue(result._configuration().connectionConfigByName().contains("my_postgres"));
        assertTrue(result._configuration().connectionConfigByName().get("my_postgres").get().contains(Constants.URL()));
        assertTrue(result._configuration().connectionConfigByName().get("my_postgres").get().get(Constants.URL()).contains("jdbc:url"));
        assertTrue(result._validations().isEmpty());
    }

}