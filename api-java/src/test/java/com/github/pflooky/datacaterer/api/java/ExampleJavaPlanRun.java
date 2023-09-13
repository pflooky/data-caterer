package com.github.pflooky.datacaterer.api.java;

public class ExampleJavaPlanRun extends PlanRun {
    {
        var myJson = json("minimal_json", "app/src/test/resources/sample/json/minimal")
                .schema(schema().addFields(field().name("account_id")));
        var myPostgres = postgres("my_postgres", "url")
                .table("my.table")
                .schema(field().name("account_id"));
        var mySecondPostgres = postgres(myPostgres)
                .table("my.othertable")
                .schema(field().name("acc_id"));
        execute(myJson);
    }
}
