package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.model.ForeignKeyRelation
import org.scalatest.funsuite.AnyFunSuite

class SinkOptionsBuilderTest extends AnyFunSuite {

  test("Can create sink options with random seed, locale and foreign keys") {
    val result = SinkOptionsBuilder()
      .seed(10)
      .locale("id")
      .foreignKey(ForeignKeyRelation("my_postgres", "account", "account_id"),
        ForeignKeyRelation("my_json", "account", "account_id"))
      .foreignKey(ForeignKeyRelation("my_postgres", "account", "customer_number"),
        ForeignKeyRelation("my_json", "account", "customer_number"),
        ForeignKeyRelation("my_parquet", "transaction", "cust_num"))
      .sinkOptions

    assert(result.seed.contains("10"))
    assert(result.locale.contains("id"))
    assert(result.foreignKeys.size == 2)
    assert(result.foreignKeys.exists(_ == "my_postgres.account.account_id" -> List("my_json.account.account_id")))
    assert(result.foreignKeys.exists(_ == "my_postgres.account.customer_number" -> List("my_json.account.customer_number", "my_parquet.transaction.cust_num")))
  }

}
