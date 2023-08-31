package com.github.pflooky.datacaterer.api

class ExamplePlanRun extends PlanRun {

  val planBuilder = plan.name("sample plan")

  val tasksBuilder = tasks.
    addTask("account_json", "fs_json",
      step.name("account")
        .option(("path", "app/src/test/resources/sample/json/account"))
        .schema(schema.addFields(List(
          field.name("account_id"),
          field.name("year").`type`("int").min(2022),
          field.name("name").static("peter")
        )))
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