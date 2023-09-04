package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.model.{ArrayType, DateType, DoubleType, IntegerType}

import java.sql.Date

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

  execute(List(tasksBuilder), planBuilder)
}

class MinimalPlanRun extends PlanRun {
  execute(configuration =
    configuration
      .enableGeneratePlanAndTasks(true)
      .addConnectionConfig("account_json", "json", Map("path" -> "app/src/test/resources/sample/json/account"))
  )
}

class MinimalPlanWithManualTaskRun extends PlanRun {
  val tasksBuilder = tasks.addTask("my_task", "mininal_json",
    step
      .option(("path", "app/src/test/resources/sample/json/minimal"))
      .schema(schema.addField(field.name("account_id")))
  )
  execute(tasksBuilder)
}


class LargeCountRun extends PlanRun {
  val tasksBuilder = tasks.addTask("my_task", "mininal_json",
    step
      .option(("path", "app/src/test/resources/sample/json/large"))
      .schema(schema.addFields(
        field.name("account_id"),
        field.name("year").`type`(IntegerType).min(2022),
        field.name("name").expression("#{Name.name}"),
        field.name("amount").`type`(DoubleType).max(1000.0),
        field.name("date").`type`(DateType).min(Date.valueOf("2022-01-01")),
        field.name("status").oneOf("open", "closed"),
        field.name("txn_list")
          .`type`(ArrayType)
          .schema(schema.addFields(
            field.name("id"),
            field.name("date").`type`(DateType).min(Date.valueOf("2022-01-01")),
            field.name("amount").`type`(DoubleType),
          ))
      ))
      .count(count
        .total(1000)
        .columns("account_id")
        .perColumnTotal(100)
      )
  )

  val conf = configuration.numRecordsPerBatch(1000000)
  execute(List(tasksBuilder), configuration = conf)
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

class FullExamplePlanRun extends PlanRun {

  val startDate = Date.valueOf("2022-01-01")
  val accountIdField = field.name("account_id").regex("ACC[0-9]{8}")
  val nameField = field.name("name").expression("#{Name.name}")

  val postgresTask = task.name("postgres_account_details")
    .steps(
      step
        .name("transaction")
        .jdbcTable("account.transaction")
        .schema(schema.addFields(
          accountIdField,
          field.name("txn_id").regex("txn_[0-9]{5}"),
          field.name("year").`type`(IntegerType).sql("YEAR(date)"),
          nameField,
          field.name("date").`type`(DateType).min(startDate),
          field.name("amount").`type`(DoubleType).max(10000),
          field.name("credit_debit").sql("CASE WHEN amount < 0 THEN 'C' ELSE 'D' END"),
        )),
      step
        .name("account")
        .jdbcTable("account.account")
        .schema(schema.addFields(
          accountIdField,
          nameField,
          field.name("open_date").`type`(DateType).min(startDate),
          field.name("status").oneOf("open", "closed", "pending")
        ))
    )

  val jsonTask = task.name("json_account_details")
    .step(
      step
        .name("account_info")
        .path("/tmp/src/main/resources/sample/json")
        .schema(schema.addFields(
          accountIdField,
          nameField,
          field.name("txn_list")
            .`type`(ArrayType)
            .schema(schema.addFields(
              field.name("id"),
              field.name("date").`type`(DateType).min(startDate),
              field.name("amount").`type`(DoubleType),
            ))
        ))
    )

  val conf = configuration
    .postgres("customer_postgres")
    .json("account_json")

  val p = plan.taskSummaries(
    taskSummary.dataSourceName("customer_postgres").task(postgresTask),
    taskSummary.dataSourceName("account_json").task(jsonTask),
  ).addForeignKeyRelationship(
    foreignField("customer_postgres", "account", "account_id"),
    foreignField("customer_postgres", "transaction", "account_id")
  ).addForeignKeyRelationship(
    foreignField("customer_postgres", "account", "account_id"),
    foreignField("account_json", "account_info", "account_id")
  )

  execute(p, conf)
}