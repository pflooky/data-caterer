package com.github.pflooky.datagen.core.util

import com.github.pflooky.datagen.core.generator.metadata.datasource.database.{ColumnMetadata, PostgresMetadata}
import com.github.pflooky.datagen.core.model.Constants.ONE_OF_GENERATOR
import com.github.pflooky.datagen.core.model.MetadataConfig
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.{Encoder, Encoders}

import java.sql.Date

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

    assert(result.length == 4)
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
      Account("acc123", "peter", Date.valueOf("2023-01-01"), 10),
      Account("acc124", "john", Date.valueOf("2023-01-02"), 49),
      Account("acc125", "peter", Date.valueOf("2023-02-02"), 21),
      Account("acc126", "john", Date.valueOf("2023-02-04"), 26),
    ))
    val dataSourceMetadata = PostgresMetadata("postgres", Map("url" -> "localhost"))

    val result = MetadataUtil.getFieldDataProfilingMetadata(df, Map(), dataSourceMetadata, MetadataConfig(100, 100, 0.5))

    assert(result.size == 4)
    val accountIdField = result.find(_.columnName == "account_id").get
    assert(accountIdField.metadata == Map("count" -> "4", "distinctCount" -> "4", "version" -> "2", "maxLen" -> "6", "avgLen" -> "6", "nullCount" -> "0"))
    val nameField = result.find(_.columnName == "name").get
    assert(nameField.metadata == Map("count" -> "4", "distinctCount" -> "2", "version" -> "2", "maxLen" -> "5", "avgLen" -> "5", "nullCount" -> "0", ONE_OF_GENERATOR -> Some(Array("peter", "john"))))
    val dateField = result.find(_.columnName == "open_date").get
    assert(dateField.metadata == Map("count" -> "4", "distinctCount" -> "4", "min" -> "2023-01-01", "version" -> "2", "max" -> "2023-02-04", "maxLen" -> "4", "avgLen" -> "4", "nullCount" -> "0"))
    val amountField = result.find(_.columnName == "age").get
    assert(amountField.metadata == Map("count" -> "4", "distinctCount" -> "4", "min" -> "10", "version" -> "2", "max" -> "49", "maxLen" -> "4", "avgLen" -> "4", "nullCount" -> "0"))
  }
}
