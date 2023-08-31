package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.model.{Count, Field, Generator, Step, Task}
import org.scalatest.funsuite.AnyFunSuite

class TasksBuilderTest extends AnyFunSuite {

  test("Can create a task summary when given a task") {
    val result = TaskSummaryBuilder()
      .task(TaskBuilder().name("my task"))
      .enabled(false)
      .dataSourceName("account_json")
      .taskSummary

    assert(result.name == "my task")
    assert(result.dataSourceName == "account_json")
    assert(!result.enabled)
  }

  test("Can create a task with defaults when given a step") {
    val result = TasksBuilder().addTask(StepBuilder().name("my step").`type`("postgres")).tasks

    assert(result.size == 1)
    assert(result.head.task.name == Task().name)
    assert(result.head.task.steps.size == 1)
    assert(result.head.task.steps.head == Step("my step", "postgres"))
  }

  test("Can create a step with details") {
    val result = StepBuilder()
      .name("my step")
      .`type`("csv")
      .enabled(false)
      .schema(SchemaBuilder())
      .count(CountBuilder())
      .option("dbtable" -> "account.history")
      .options(Map("stringtype" -> "undefined"))
      .step

    assert(result.name == "my step")
    assert(result.`type` == "csv")
    assert(!result.enabled)
    assert(result.schema.fields.isEmpty)
    assert(result.count == Count())
    assert(result.options == Map(
      "dbtable" -> "account.history",
      "stringtype" -> "undefined"
    ))
  }

  test("Can create simple count") {
    val result = CountBuilder().total(20).count

    assert(result.total.contains(20))
    assert(result.perColumn.isEmpty)
    assert(result.generator.isEmpty)
  }

  test("Can create per column count") {
    val result = CountBuilder()
      .perColumn(PerColumnCountBuilder()
        .total(20)
        .columns(List("account_id"))
      )
      .count

    assert(result.total.contains(1000))
    assert(result.perColumn.isDefined)
    assert(result.perColumn.get.count.contains(20))
    assert(result.perColumn.get.columnNames == List("account_id"))
    assert(result.perColumn.get.generator.isEmpty)
    assert(result.generator.isEmpty)
  }

  test("Can create per column count with generator") {
    val result = CountBuilder()
      .perColumn(PerColumnCountBuilder()
        .columns(List("account_id"))
        .generator(GeneratorBuilder().min(5))
      ).count

    assert(result.total.contains(1000))
    assert(result.perColumn.isDefined)
    assert(result.perColumn.get.count.contains(10))
    assert(result.perColumn.get.columnNames == List("account_id"))
    assert(result.perColumn.get.generator.isDefined)
    assert(result.perColumn.get.generator.get.`type` == "random")
    assert(result.perColumn.get.generator.get.options("min") == "5")
    assert(result.generator.isEmpty)
  }

  test("Can create schema with add fields") {
    val result = SchemaBuilder()
      .addField("account_id", "string")
      .addField("year", "int")
      .addFields(List(FieldBuilder().name("name").`type`("string")))
      .schema

    assert(result.fields.isDefined)
    assert(result.fields.get.size == 3)
    assert(result.fields.get.contains(Field("account_id", Some("string"))))
    assert(result.fields.get.contains(Field("year", Some("int"))))
    assert(result.fields.get.contains(Field("name", Some("string"))))
  }

  test("Can create schema with fields") {
    val result = SchemaBuilder()
      .addField("account_id", "string")
      .fields(List(FieldBuilder().name("name").`type`("string")))
      .addField("year", "int")
      .schema

    assert(result.fields.isDefined)
    assert(result.fields.get.size == 2)
    assert(result.fields.get.contains(Field("year", Some("int"))))
    assert(result.fields.get.contains(Field("name", Some("string"))))
  }

  test("Can create field") {
    val result = FieldBuilder()
      .name("account_id")
      .`type`("string")
      .nullable(false)
      .generator(GeneratorBuilder())
      .field

    assert(result.name == "account_id")
    assert(result.`type`.contains("string"))
    assert(!result.nullable)
    assert(result.generator.isDefined)
    assert(result.generator.contains(Generator()))
  }

  test("Can create field generated from sql expression") {
    val result = FieldBuilder()
      .name("account_id")
      .sql("SUBSTRING(account, 1, 5)")
      .field

    assert(result.name == "account_id")
    assert(result.`type`.contains("string"))
    assert(result.generator.isDefined)
    assert(result.generator.get.`type` == "sql")
    assert(result.generator.get.options("sql") == "SUBSTRING(account, 1, 5)")
  }

  test("Can create field generated from one of list") {
    val result = FieldBuilder()
      .name("account_id")
      .oneOf(List(123.1, 789.2))
      .field

    assert(result.name == "account_id")
    assert(result.`type`.contains("double"))
    assert(result.generator.isDefined)
    assert(result.generator.get.options("oneOf") == List(123.1, 789.2))
  }

  test("Can create field with metadata") {
    val result = FieldBuilder()
      .name("account_id")
      .regex("acc[0-9]{3}")
      .seed(1)
      .min(2)
      .max(10)
      .minLength(3)
      .maxLength(4)
      .avgLength(3)
      .listMinLength(2)
      .listMaxLength(2)
      .expression("hello")
      .nullable(false)
      .static("acc123")
      .arrayType("boolean")
      .numericPrecision(10)
      .numericScale(1)
      .enableEdgeCases(true)
      .edgeCaseProbability(0.5)
      .enableNull(true)
      .nullProbability(0.1)
      .unique(true)
      .omit(false)
      .primaryKey(true)
      .primaryKeyPosition(1)
      .clusteringPosition(1)
      .options(Map("customMetadata" -> "yes"))
      .option("data" -> "big")
      .field

    assert(result.name == "account_id")
    assert(result.`type`.contains("string"))
    assert(!result.nullable)
    assert(result.generator.get.`type` == "regex")
    val gen = result.generator.get.options
    assert(gen("regex") == "acc[0-9]{3}")
    assert(gen("seed") == "1")
    assert(gen("min") == "2")
    assert(gen("max") == "10")
    assert(gen("minLen") == "3")
    assert(gen("maxLen") == "4")
    assert(gen("avgLen") == "3")
    assert(gen("listMinLen") == "2")
    assert(gen("listMaxLen") == "2")
    assert(gen("expression") == "hello")
    assert(gen("static") == "acc123")
    assert(gen("arrayType") == "boolean")
    assert(gen("precision") == "10")
    assert(gen("scale") == "1")
    assert(gen("enableEdgeCases") == "true")
    assert(gen("edgeCaseProb") == "0.5")
    assert(gen("enableNull") == "true")
    assert(gen("nullProb") == "0.1")
    assert(gen("isUnique") == "true")
    assert(gen("omit") == "false")
    assert(gen("isPrimaryKey") == "true")
    assert(gen("primaryKeyPos") == "1")
    assert(gen("clusteringPos") == "1")
    assert(gen("customMetadata") == "yes")
    assert(gen("data") == "big")
  }

}
