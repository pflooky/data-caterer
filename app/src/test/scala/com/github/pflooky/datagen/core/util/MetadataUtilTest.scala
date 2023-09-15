package com.github.pflooky.datagen.core.util

import com.github.pflooky.datacaterer.api.model.Constants.ONE_OF_GENERATOR
import com.github.pflooky.datacaterer.api.model.MetadataConfig
import com.github.pflooky.datagen.core.generator.metadata.datasource.database.{ColumnMetadata, PostgresMetadata}
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.{Encoder, Encoders}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.sql.Date

@RunWith(classOf[JUnitRunner])
class MetadataUtilTest extends SparkSuite {

  test("Can convert metadata to map") {
    val metadata = new MetadataBuilder()
      .putString("string_key", "value")
      .putLong("long_key", 1L)
      .putDouble("double_key", 0.1)
      .putBoolean("boolean_key", true)
      .putStringArray("array_key", Array("value"))
      .build()

    val result = MetadataUtil.metadataToMap(metadata)

    assert(result.size == 5)
    assert(List("string_key", "long_key", "double_key", "boolean_key", "array_key").forall(result.contains))
  }

  test("Able to create array of struct fields based on metadata gathered") {
    implicit val encoder: Encoder[ColumnMetadata] = Encoders.kryo[ColumnMetadata]
    val readOptions = Map("dbtable" -> "account.accounts")
    val df = sparkSession.createDataFrame(Seq(Account("acc123", "peter", Date.valueOf("2023-01-01"), 10)))
    val dataProfilingMetadata = List(
      DataProfilingMetadata("account_id", Map("minLen" -> "2", "maxLen" -> "10")),
      DataProfilingMetadata("name", Map("distinctCount" -> "2", "count" -> "100", ONE_OF_GENERATOR -> Some(Array("peter", "john")))),
      DataProfilingMetadata("open_date", Map()),
    )
    val columnMetadata = sparkSession.createDataset(Seq(
      ColumnMetadata("account_id", readOptions, Map("sourceDataType" -> "varchar")),
      ColumnMetadata("open_date", readOptions, Map("isNullable" -> "false")),
    ))

    val result = MetadataUtil.mapToStructFields(df, readOptions, dataProfilingMetadata, columnMetadata)

    assert(result.length == 5)
    result.find(_.name == "account_id")
      .foreach(s => {
        assert(s.metadata.getString("minLen") == "2")
        assert(s.metadata.getString("maxLen") == "10")
        assert(s.metadata.getString("sourceDataType") == "varchar")
        assert(s.nullable)
      })
    result.find(_.name == "name")
      .foreach(s => {
        assert(s.metadata.contains("oneOf"))
        assert(s.metadata.getStringArray("oneOf") sameElements Array("peter", "john"))
        assert(s.metadata.getString("distinctCount") == "2")
        assert(s.metadata.getString("count") == "100")
        assert(s.nullable)
      })
    result.find(_.name == "open_date")
      .foreach(s => {
        assert(!s.nullable)
      })
  }

  test("Can calculate data profiling statistics from data frame") {
    val df = sparkSession.createDataFrame(Seq(
      Account("acc123", "peter", Date.valueOf("2023-01-01"), 10, "D"),
      Account("acc124", "john", Date.valueOf("2023-01-02"), 49, "D"),
      Account("acc125", "peter", Date.valueOf("2023-02-02"), 21, "C"),
      Account("acc126", "john", Date.valueOf("2023-02-04"), 26, "C"),
    ))
    val dataSourceMetadata = PostgresMetadata("postgres", Map("url" -> "localhost"))

    val result = MetadataUtil.getFieldDataProfilingMetadata(df, Map(), dataSourceMetadata, MetadataConfig(100, 100, 0.5, 1))

    assert(result.size == 5)
    val accountIdField = result.find(_.columnName == "account_id").get
    assertResult(Map("count" -> "4", "distinctCount" -> "4", "maxLen" -> "6", "avgLen" -> "6", "nullCount" -> "0"))(accountIdField.metadata)
    val nameField = result.find(_.columnName == "name").get
    val nameMeta = Map("count" -> "4", "distinctCount" -> "2", "maxLen" -> "5", "avgLen" -> "5", "nullCount" -> "0", ONE_OF_GENERATOR -> Array("peter", "john"), "expression" -> "#{Name.name}")
    nameMeta.foreach(m => assertResult(m._2)(nameField.metadata(m._1)))
    val dateField = result.find(_.columnName == "open_date").get
    assertResult(Map("count" -> "4", "distinctCount" -> "4", "min" -> "2023-01-01", "max" -> "2023-02-04", "maxLen" -> "4", "avgLen" -> "4", "nullCount" -> "0"))(dateField.metadata)
    val amountField = result.find(_.columnName == "age").get
    assertResult(Map("count" -> "4", "distinctCount" -> "4", "min" -> "10", "max" -> "49", "maxLen" -> "4", "avgLen" -> "4", "nullCount" -> "0"))(amountField.metadata)
    val debitCreditField = result.find(_.columnName == "debitCredit").get
    val debitCreditMeta = Map("count" -> "4", "distinctCount" -> "2", "maxLen" -> "1", "avgLen" -> "1", "nullCount" -> "0", ONE_OF_GENERATOR -> Array("D", "C"))
    debitCreditMeta.foreach(m => assertResult(m._2)(debitCreditField.metadata(m._1)))
  }
}
