package com.github.pflooky.datacaterer.api.java;

public class ExampleJavaPlanRun extends PlanRun {
    {
        var myJson = json("minimal_json", "app/src/test/resources/sample/json/minimal")
                .schema(schema().addFields(field().name("account_id")));
        execute(myJson);
    }
}
