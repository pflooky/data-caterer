package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.model.{ArrayType, DateType, DoubleType, IntegerType}

import java.sql.Date

class DocumentationPlanRun extends PlanRun {
  val jsonTask = json("account_info", "/tmp/json")
    .schema(
      field.name("account_id").regex("ACC[0-9]{8}"),
      field.name("year").`type`(IntegerType).sql("YEAR(date)"),
      field.name("name").expression("#{Name.name}"),
      field.name("amount").`type`(DoubleType).min(10).max(1000),
      field.name("date").`type`(DateType).min(Date.valueOf("2022-01-01")),
      field.name("status").oneOf("open", "closed"),
      field.name("txn_list")
        .`type`(ArrayType)
        .schema(schema.addFields(
          field.name("id").sql("_join_txn_id"),
          field.name("date").`type`(DateType).min(Date.valueOf("2022-01-01")),
          field.name("amount").`type`(DoubleType)
        )),
      field.name("_join_txn_id").omit(true)
    )
    .count(count.records(100))
  //count.recordsPerColumn
  //count.records
  //hide away spark config
  //foreign key based on task and column
  //example showing all base features

  val csvTxns = csv("transactions", "/tmp/csv")
    .schema(
      field.name("account_id"),
      field.name("txn_id"),
      field.name("amount"),
      field.name("merchant").expression("#{Company.name}"),
    )
    .count(count.perColumnGenerator(generator.min(1).max(5), "account_id"))

  val foreignKeySetup = plan
    .addForeignKeyRelationship(jsonTask, "account_id", List((csvTxns, "account_id")))
    .addForeignKeyRelationship(jsonTask, "_join_txn_id", List((csvTxns, "txn_id")))
    .addForeignKeyRelationship(jsonTask, "amount", List((csvTxns, "amount")))

  execute(foreignKeySetup, configuration, jsonTask, csvTxns)
}
