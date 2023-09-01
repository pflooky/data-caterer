package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.model.IntegerType

class ExamplePlanRun extends PlanRun {

  val planBuilder = plan.name("sample plan")

  val tasksBuilder = tasks.
    addTask("account_json", "fs_json",
      step.name("account")
        .option(("path", "app/src/test/resources/sample/json/account"))
        .schema(schema.addFields(
          field.name("account_id"),
          field.name("year").`type`(IntegerType).min(2022),
          field.name("name").static("peter")
        ))
    )

  execute(tasksBuilder, planBuilder)
}

class MinimalPlanRun extends PlanRun {
  execute(configuration =
    configuration
      .enableGeneratePlanAndTasks(true)
      .addConnectionConfig("account_json", "json", Map("path" -> "app/src/test/resources/sample/json/account"))
  )
}

class MinimalPlanWithManualTaskRun extends PlanRun {
  val tasksBuilder = tasks.addTask(
    step
      .option(("path", "app/src/test/resources/sample/json/minimal"))
      .schema(schema.addField(field.name("account_id")))
  )
  execute(tasksBuilder)
}

class DocsPlanRun extends PlanRun {

  validationConfig
    .name("account_checks")
    .description("Check account related fields have gone through system correctly")
    .addValidations(
      "accountJson",
      Map("path" -> "sample/json/txn-gen"),
      validation.expr("amount < 100"),
      validation.expr("year == 2021").errorThreshold(0.1),
      validation.expr("regexp_like(name, 'Peter .*')").errorThreshold(200).description("Should be lots of Peters")
    )
  val t = task
    .name("csv_file")
    .step(
      step
        .name("transactions")
        .`type`("csv")
        .option("path", "app/src/test/resources/sample/csv/transactions")
        .count(
          count
            .total(1000)
            .perColumnGenerator(
              generator
                .min(1)
                .max(2)
            )
        )
    )
}