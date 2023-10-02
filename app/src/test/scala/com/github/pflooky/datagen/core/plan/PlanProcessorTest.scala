package com.github.pflooky.datagen.core.plan

import com.github.pflooky.datacaterer.api.PlanRun
import com.github.pflooky.datacaterer.api.model.Constants.SAVE_MODE
import com.github.pflooky.datacaterer.api.model.{ArrayType, DateType, DoubleType, IntegerType, TimestampType}
import com.github.pflooky.datagen.core.util.{ObjectMapperUtil, SparkSuite}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.sql.{Date, Timestamp}

@RunWith(classOf[JUnitRunner])
class PlanProcessorTest extends SparkSuite {

  private val scalaBaseFolder = "src/test/resources/sample/documentation"
  private val javaBaseFolder = "src/test/resources/sample/java/documentation"

  class DocumentationPlanRun extends PlanRun {
    {
      val accountStatus = List("open", "closed", "pending", "suspended")
      val jsonTask = json("account_info", s"$scalaBaseFolder/json", Map(SAVE_MODE -> "overwrite"))
        .schema(
          field.name("account_id").regex("ACC[0-9]{8}"),
          field.name("year").`type`(IntegerType).sql("YEAR(date)"),
          field.name("balance").`type`(DoubleType).min(10).max(1000),
          field.name("date").`type`(DateType).min(Date.valueOf("2022-01-01")),
          field.name("status").oneOf(accountStatus: _*),
          field.name("update_history")
            .`type`(ArrayType)
            .schema(
              field.name("updated_time").`type`(TimestampType).min(Timestamp.valueOf("2022-01-01 00:00:00")),
              field.name("prev_status").oneOf(accountStatus: _*),
              field.name("new_status").oneOf(accountStatus: _*)
            ),
          field.name("customer_details")
            .schema(
              field.name("name").sql("_join_txn_name"),
              field.name("age").`type`(IntegerType).min(18).max(90),
              field.name("city").expression("#{Address.city}")
            ),
          field.name("_join_txn_name").expression("#{Name.name}").omit(true)
        )
        .count(count.records(100))

      val csvTxns = csv("transactions", s"$scalaBaseFolder/csv", Map(SAVE_MODE -> "overwrite", "header" -> "true"))
        .schema(
          field.name("account_id"),
          field.name("txn_id"),
          field.name("name"),
          field.name("amount").`type`(DoubleType).min(10).max(100),
          field.name("merchant").expression("#{Company.name}"),
          field.name("time").`type`(TimestampType),
          field.name("date").`type`(DateType).sql("DATE(time)"),
        )
        .count(
          count
            .records(100)
            .recordsPerColumnGenerator(generator.min(1).max(2), "account_id", "name")
        )
        .validationWait(waitCondition.pause(1))
        .validations(
          validation.expr("amount > 0").errorThreshold(0.01),
          validation.expr("LENGTH(name) > 3").errorThreshold(5),
          validation.expr("LENGTH(merchant) > 0").description("Non-empty merchant name"),
        )

      val foreignKeySetup = plan
        .addForeignKeyRelationship(
          jsonTask, List("account_id", "_join_txn_name"),
          List((csvTxns, List("account_id", "name")))
        )
      val conf = configuration
        .generatedReportsFolderPath(s"$scalaBaseFolder/report")
        .enableValidation(true)
        .enableSinkMetadata(true)

      execute(foreignKeySetup, conf, jsonTask, csvTxns)
    }
  }

  test("Can run documentation plan run") {
    PlanProcessor.determineAndExecutePlan(Some(new DocumentationPlanRun()))
    verifyGeneratedData(scalaBaseFolder)
  }

  ignore("Can run Java plan run") {
    PlanProcessor.determineAndExecutePlanJava(new ExampleJavaPlanRun(javaBaseFolder))
    verifyGeneratedData(javaBaseFolder)
  }

  private def verifyGeneratedData(folder: String) = {
    val jsonData = sparkSession.read.json(s"$folder/json").selectExpr("*", "customer_details.name AS name").collect()
    val csvData = sparkSession.read.option("header", "true").csv(s"$folder/csv").collect()
    val csvCount = csvData.length
    assert(jsonData.length == 100)
    assert(csvCount >= 100 && csvCount <= 200)
    val jsonRecord = jsonData.head
    val jsonAccountId = jsonRecord.getString(0)
    val csvMatchAccount = csvData.filter(r => r.getString(0).equalsIgnoreCase(jsonAccountId))
    val csvMatchCount = csvMatchAccount.length
    assert(csvMatchCount >= 1 && csvMatchCount <= 2)
    assert(csvMatchAccount.forall(r => r.getAs[String]("name").equalsIgnoreCase(jsonRecord.getAs[String]("name"))))
    assert(csvData.forall(r => r.getAs[String]("time").substring(0, 10) == r.getAs[String]("date")))
  }

  ignore("Write YAML for plan") {
    val docPlanRun = new DocumentationPlanRun()
    val planWrite = ObjectMapperUtil.yamlObjectMapper.writeValueAsString(docPlanRun._plan)
    println(planWrite)
  }

  class TestPostgres extends PlanRun {
    val csvTask = csv("my_csv", "/tmp/data/csv", Map("saveMode" -> "overwrite", "header" -> "true"))
      .schema(metadataSource.marquez("http://localhost:5001", "food_delivery", "public.categories"))
      .count(count.records(100))

    val jsonTask = json("my_json", "/tmp/data/json", Map("saveMode" -> "overwrite"))
      .schema(metadataSource.marquez("http://localhost:5001", "food_delivery", "public.categories"))
      .schema(field.name("name").expression("#{Name.name}"))
      .count(count.recordsPerColumn(2, "name"))

    val postgresTask = postgres("my_postgres", "jdbc:postgresql://localhost:5432/food_delivery", "postgres", "password")
      .schema(metadataSource.marquez("http://localhost:5001", "food_delivery"))
      .count(count.records(10))

    val conf = configuration.enableGeneratePlanAndTasks(true).enableFailOnError(false)

    execute(conf, csvTask, jsonTask, postgresTask)
  }

  ignore("Can run Postgres plan run") {
//    PlanProcessor.determineAndExecutePlan(Some(new TestPostgres()))
    PlanProcessor.determineAndExecutePlan(Some(new TestValidation()))
  }

  class TestValidation extends PlanRun {
    val csvTask = csv("my_csv", "/tmp/data/csv", Map("saveMode" -> "overwrite", "header" -> "true"))
      .schema(
        field.name("account_id").regex("ACC[0-9]{8}"),
        field.name("name").expression("#{Name.name}"),
        field.name("amount").`type`(IntegerType).min(0).max(100)
      )
      .count(count.recordsPerColumn(5, "account_id"))
      .validations(
        validation.col("transactions").hasType("string"),
        validation.col("amount").isEqual(1000),
        validation.groupBy("account_id").count("amount").lessThan(1000),
        validation.groupBy("account_id").sum("amount").greaterThan(1000)
      )

    val conf = configuration.enableValidation(true)
      .generatedReportsFolderPath("/Users/peter/code/spark-datagen/gen/report")

    execute(conf, csvTask)
  }
}
