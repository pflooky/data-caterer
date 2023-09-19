package com.github.pflooky.datagen.core.plan;

import com.github.pflooky.datacaterer.api.model.ArrayType;
import com.github.pflooky.datacaterer.api.model.Constants;
import com.github.pflooky.datacaterer.api.model.DateType;
import com.github.pflooky.datacaterer.api.model.DoubleType;
import com.github.pflooky.datacaterer.api.model.IntegerType;
import com.github.pflooky.datacaterer.api.model.TimestampType;
import com.github.pflooky.datacaterer.java.api.PlanRun;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class ExampleJavaPlanRun extends PlanRun {
    private String baseFolder;

    public ExampleJavaPlanRun(String baseFolder) {
        this.baseFolder = baseFolder;
    }

    {
        String[] accountStatus = {"open", "closed", "pending", "suspended"};
        var jsonTask = json("account_info", baseFolder + "/json", Map.of(Constants.SAVE_MODE(), "overwrite"))
                .schema(
                        field().name("account_id").regex("ACC[0-9]{8}"),
                        field().name("year").type(IntegerType.instance()).sql("YEAR(date)"),
                        field().name("balance").type(DoubleType.instance()).min(10).max(1000),
                        field().name("date").type(DateType.instance()).min(Date.valueOf("2022-01-01")),
                        field().name("status").oneOf(accountStatus),
                        field().name("update_history")
                                .type(ArrayType.instance())
                                .schema(
                                        field().name("updated_time").type(TimestampType.instance()).min(Timestamp.valueOf("2022-01-01 00:00:00")),
                                        field().name("prev_status").oneOf(accountStatus),
                                        field().name("new_status").oneOf(accountStatus)
                                ),
                        field().name("customer_details")
                                .schema(
                                        field().name("name").sql("_join_txn_name"),
                                        field().name("age").type(IntegerType.instance()).min(18).max(90),
                                        field().name("city").expression("#{Address.city}")
                                ),
                        field().name("_join_txn_name").expression("#{Name.name}").omit(true)
                )
                .count(count().records(100));

        var csvTxns = csv("transactions", baseFolder + "/csv", Map.of(Constants.SAVE_MODE(), "overwrite", "header", "true"))
                .schema(
                        field().name("account_id"),
                        field().name("txn_id"),
                        field().name("name"),
                        field().name("amount").type(DoubleType.instance()).min(10).max(100),
                        field().name("merchant").expression("#{Company.name}")
                )
                .count(
                        count()
                                .recordsPerColumnGenerator(100, generator().min(1).max(2), "account_id", "name")
                )
                .validationWait(waitCondition().pause(1))
                .validations(
                        validation().expr("amount > 0").errorThreshold(0.01),
                        validation().expr("LENGTH(name) > 3").errorThreshold(5),
                        validation().expr("LENGTH(merchant) > 0").description("Non-empty merchant name")
                );

        var foreignKeySetup = plan()
                .addForeignKeyRelationship(
                        jsonTask, List.of("account_id", "_join_txn_name"),
                        List.of(Map.entry(csvTxns, List.of("account_id", "name")))
                );
        var conf = configuration()
                .generatedReportsFolderPath(baseFolder + "/report")
                .enableValidation(true);

        execute(foreignKeySetup, conf, jsonTask, csvTxns);
    }
}
