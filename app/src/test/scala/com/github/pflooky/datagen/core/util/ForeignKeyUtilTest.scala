package com.github.pflooky.datagen.core.util

import com.github.pflooky.datagen.core.model.SinkOptions
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.sql.Date
import java.time.LocalDate

@RunWith(classOf[JUnitRunner])
class ForeignKeyUtilTest extends SparkSuite {

  test("When no foreign keys defined, return back same dataframes") {
    val sinkOptions = SinkOptions(None, None, Map())
    val dfMap = Map("name" -> sparkSession.emptyDataFrame)

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(sinkOptions, dfMap)

    assert(dfMap == result)
  }

  test("Can link foreign keys between data sets") {
    val sinkOptions = SinkOptions(None, None, Map("postgres.account.account_id" -> List("postgres.transaction.account_id")))
    val accountsList = List(
      Account("acc1", "peter", Date.valueOf(LocalDate.now())),
      Account("acc2", "john", Date.valueOf(LocalDate.now())),
      Account("acc3", "jack", Date.valueOf(LocalDate.now()))
    )
    val transactionList = List(
      Transaction("some_acc9", "id123", Date.valueOf(LocalDate.now()), 10.0),
      Transaction("some_acc9", "id124", Date.valueOf(LocalDate.now()), 23.9),
      Transaction("some_acc10", "id125", Date.valueOf(LocalDate.now()), 85.1),
    )
    val dfMap = Map(
      "postgres.account" -> sparkSession.createDataFrame(accountsList),
      "postgres.transaction" -> sparkSession.createDataFrame(transactionList)
    )

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(sinkOptions, dfMap)
    result("postgres.transaction").show()
    val resTxnRows = result("postgres.transaction").collect()
    resTxnRows.foreach(r => {
      r.getString(0) == "acc1" || r.getString(0) == "acc2" || r.getString(0) == "acc3"
    })
  }

  test("Can get delete order based on foreign keys defined") {
    val foreignKeys = Map(
      "postgres.accounts.account_id" -> List("postgres.balances.account_id", "postgres.transactions.account_id")
    )
    val deleteOrder = ForeignKeyUtil.getDeleteOrder(foreignKeys)
    assert(deleteOrder == List("postgres.balances.account_id", "postgres.transactions.account_id", "postgres.accounts.account_id"))
  }

  test("Can get delete order based on nested foreign keys") {
    val foreignKeys = Map(
      "postgres.accounts.account_id" -> List("postgres.balances.account_id"),
      "postgres.balances.account_id" -> List("postgres.transactions.account_id"),
    )
    val deleteOrder = ForeignKeyUtil.getDeleteOrder(foreignKeys)
    val expected = List("postgres.transactions.account_id", "postgres.balances.account_id", "postgres.accounts.account_id")
    assert(deleteOrder == expected)

    val foreignKeys1 = Map(
      "postgres.balances.account_id" -> List("postgres.transactions.account_id"),
      "postgres.accounts.account_id" -> List("postgres.balances.account_id"),
    )
    val deleteOrder1 = ForeignKeyUtil.getDeleteOrder(foreignKeys1)
    assert(deleteOrder1 == expected)

    val foreignKeys2 = Map(
      "postgres.accounts.account_id" -> List("postgres.balances.account_id"),
      "postgres.balances.account_id" -> List("postgres.transactions.account_id"),
      "postgres.transactions.account_id" -> List("postgres.customer.account_id"),
    )
    val deleteOrder2 = ForeignKeyUtil.getDeleteOrder(foreignKeys2)
    val expected2 = List("postgres.customer.account_id") ++ expected
    assert(deleteOrder2 == expected2)
  }

}

case class Account(account_id: String = "acc123", name: String = "peter", open_date: Date = Date.valueOf("2023-01-31"), age: Int = 10)

case class Transaction(account_id: String, transaction_id: String, created_date: Date, amount: Double)
