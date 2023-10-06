package com.github.pflooky.datagen.core.util

import com.github.pflooky.datacaterer.api.PlanRun
import com.github.pflooky.datacaterer.api.model.Constants.FOREIGN_KEY_DELIMITER
import com.github.pflooky.datacaterer.api.model.{ForeignKeyRelation, Plan, SinkOptions, TaskSummary}
import com.github.pflooky.datagen.core.generator.metadata.datasource.database.ForeignKeyRelationship
import org.apache.spark.sql.Encoders
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.sql.Date
import java.time.LocalDate

@RunWith(classOf[JUnitRunner])
class ForeignKeyUtilTest extends SparkSuite {

  test("When no foreign keys defined, return back same dataframes") {
    val sinkOptions = SinkOptions(None, None, List())
    val plan = Plan("no foreign keys", "simple plan", List(), Some(sinkOptions))
    val dfMap = Map("name" -> sparkSession.emptyDataFrame)

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(plan, dfMap)

    assert(dfMap.toList == result)
  }

  test("Can get insert order") {
    val foreignKeys = List(
      "orders" -> List("customers"),
      "order_items" -> List("orders", "products"),
      "reviews" -> List("products", "customers")
    )
    val result = ForeignKeyUtil.getInsertOrder(foreignKeys)
    assert(result.head == "reviews")
  }

  test("Can link foreign keys between data sets") {
    val sinkOptions = SinkOptions(None, None,
      List(s"postgres${FOREIGN_KEY_DELIMITER}account${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}transaction${FOREIGN_KEY_DELIMITER}account_id"))
    )
    val plan = Plan("foreign keys", "simple plan", List(), Some(sinkOptions))
    val accountsList = List(
      Account("acc1", "peter", Date.valueOf(LocalDate.now())),
      Account("acc2", "john", Date.valueOf(LocalDate.now())),
      Account("acc3", "jack", Date.valueOf(LocalDate.now()))
    )
    val transactionList = List(
      Transaction("some_acc9", "rand1", "id123", Date.valueOf(LocalDate.now()), 10.0),
      Transaction("some_acc9", "rand2", "id124", Date.valueOf(LocalDate.now()), 23.9),
      Transaction("some_acc10", "rand3", "id125", Date.valueOf(LocalDate.now()), 85.1),
    )
    val dfMap = Map(
      "postgres.account" -> sparkSession.createDataFrame(accountsList),
      "postgres.transaction" -> sparkSession.createDataFrame(transactionList)
    )

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(plan, dfMap)
    val txn = result.filter(f => f._1.equalsIgnoreCase("postgres.transaction")).head._2
    val resTxnRows = txn.collect()
    resTxnRows.foreach(r => {
      r.getString(0) == "acc1" || r.getString(0) == "acc2" || r.getString(0) == "acc3"
    })
  }

  test("Can link foreign keys between data sets with multiple columns") {
    val sinkOptions = SinkOptions(None, None,
      List(s"postgres${FOREIGN_KEY_DELIMITER}account${FOREIGN_KEY_DELIMITER}account_id,name" -> List(s"postgres${FOREIGN_KEY_DELIMITER}transaction${FOREIGN_KEY_DELIMITER}account_id,name"))
    )
    val plan = Plan("foreign keys", "simple plan", List(TaskSummary("my_task", "postgres")), Some(sinkOptions))
    val accountsList = List(
      Account("acc1", "peter", Date.valueOf(LocalDate.now())),
      Account("acc2", "john", Date.valueOf(LocalDate.now())),
      Account("acc3", "jack", Date.valueOf(LocalDate.now()))
    )
    val transactionList = List(
      Transaction("some_acc9", "rand1", "id123", Date.valueOf(LocalDate.now()), 10.0),
      Transaction("some_acc9", "rand1", "id124", Date.valueOf(LocalDate.now()), 12.0),
      Transaction("some_acc9", "rand2", "id125", Date.valueOf(LocalDate.now()), 23.9),
      Transaction("some_acc10", "rand3", "id126", Date.valueOf(LocalDate.now()), 85.1),
    )
    val dfMap = Map(
      "postgres.account" -> sparkSession.createDataFrame(accountsList),
      "postgres.transaction" -> sparkSession.createDataFrame(transactionList)
    )

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(plan, dfMap)
    val txn = result.filter(f => f._1.equalsIgnoreCase("postgres.transaction")).head._2
    val resTxnRows = txn.collect()
    val acc1 = resTxnRows.find(_.getString(0).equalsIgnoreCase("acc1"))
    assert(acc1.isDefined)
    assert(acc1.get.getString(1).equalsIgnoreCase("peter"))
    val acc2 = resTxnRows.find(_.getString(0).equalsIgnoreCase("acc2"))
    assert(acc2.isDefined)
    assert(acc2.get.getString(1).equalsIgnoreCase("john"))
    val acc3 = resTxnRows.find(_.getString(0).equalsIgnoreCase("acc3"))
    assert(acc3.isDefined)
    assert(acc3.get.getString(1).equalsIgnoreCase("jack"))
    val acc1Count = resTxnRows.count(_.getString(0).equalsIgnoreCase("acc1"))
    val acc2Count = resTxnRows.count(_.getString(0).equalsIgnoreCase("acc2"))
    val acc3Count = resTxnRows.count(_.getString(0).equalsIgnoreCase("acc3"))
    assert(acc1Count == 2 || acc2Count == 2 || acc3Count == 2)
  }

  test("Can get delete order based on foreign keys defined") {
    val foreignKeys = List(
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id" ->
        List(s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id", s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id")
    )
    val deleteOrder = ForeignKeyUtil.getDeleteOrder(foreignKeys)
    assert(deleteOrder ==
      List(
        s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id",
        s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id",
        s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id"
      )
    )
  }

  test("Can get delete order based on nested foreign keys") {
    val foreignKeys = List(
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id"),
      s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id"),
    )
    val deleteOrder = ForeignKeyUtil.getDeleteOrder(foreignKeys)
    val expected = List(
      s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id",
      s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id",
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id"
    )
    assert(deleteOrder == expected)

    val foreignKeys1 = List(
      s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id"),
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id"),
    )
    val deleteOrder1 = ForeignKeyUtil.getDeleteOrder(foreignKeys1)
    assert(deleteOrder1 == expected)

    val foreignKeys2 = List(
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id"),
      s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id"),
      s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}customer${FOREIGN_KEY_DELIMITER}account_id"),
    )
    val deleteOrder2 = ForeignKeyUtil.getDeleteOrder(foreignKeys2)
    val expected2 = List(s"postgres${FOREIGN_KEY_DELIMITER}customer${FOREIGN_KEY_DELIMITER}account_id") ++ expected
    assert(deleteOrder2 == expected2)
  }

  test("Can generate correct values when per column count is defined over multiple columns that are also defined as foreign keys") {
    val foreignKeys = List(
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id" ->
        List(s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id", s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id")
    )
    val deleteOrder = ForeignKeyUtil.getDeleteOrder(foreignKeys)
    assert(deleteOrder == List(
      s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id",
      s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id",
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id")
    )
  }

  test("Can generate correct values when primary keys are defined over multiple columns that are also defined as foreign keys") {
    val foreignKeys = List(
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id" ->
        List(s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id", s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id")
    )
    val deleteOrder = ForeignKeyUtil.getDeleteOrder(foreignKeys)
    assert(deleteOrder == List(
      s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id",
      s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id",
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id")
    )
  }

  test("Can update foreign keys with updated names from metadata") {
    implicit val encoder = Encoders.kryo[ForeignKeyRelationship]
    val generatedForeignKeys = List(sparkSession.createDataset(Seq(ForeignKeyRelationship(
      ForeignKeyRelation("my_postgres", "public.account", List("account_id")),
      ForeignKeyRelation("my_postgres", "public.orders", List("customer_id")),
    ))))
    val optPlanRun = Some(new ForeignKeyPlanRun())
    val stepNameMapping = Map(
      s"my_csv${FOREIGN_KEY_DELIMITER}random_step" -> s"my_csv${FOREIGN_KEY_DELIMITER}public.accounts"
    )

    val result = ForeignKeyUtil.getAllForeignKeyRelationships(generatedForeignKeys, optPlanRun, stepNameMapping)

    assert(result.size == 3)
    assert(result.contains(s"my_csv${FOREIGN_KEY_DELIMITER}public.accounts${FOREIGN_KEY_DELIMITER}id" ->
      List(s"my_postgres${FOREIGN_KEY_DELIMITER}public.accounts${FOREIGN_KEY_DELIMITER}account_id")))
    assert(result.contains(s"my_json${FOREIGN_KEY_DELIMITER}json_step${FOREIGN_KEY_DELIMITER}id" ->
      List(s"my_postgres${FOREIGN_KEY_DELIMITER}public.orders${FOREIGN_KEY_DELIMITER}customer_id")))
    assert(result.contains(s"my_postgres${FOREIGN_KEY_DELIMITER}public.account${FOREIGN_KEY_DELIMITER}account_id" ->
      List(s"my_postgres${FOREIGN_KEY_DELIMITER}public.orders${FOREIGN_KEY_DELIMITER}customer_id")))
  }

  class ForeignKeyPlanRun extends PlanRun {
    val myPlan = plan.addForeignKeyRelationship(
      foreignField("my_csv", "random_step", "id"),
      foreignField("my_postgres", "public.accounts", "account_id")
    ).addForeignKeyRelationship(
      foreignField("my_json", "json_step", "id"),
      foreignField("my_postgres", "public.orders", "customer_id")
    )

    execute(plan = myPlan)
  }
}

case class Account(account_id: String = "acc123", name: String = "peter", open_date: Date = Date.valueOf("2023-01-31"), age: Int = 10, debitCredit: String = "D")

case class Transaction(account_id: String, name: String, transaction_id: String, created_date: Date, amount: Double)
