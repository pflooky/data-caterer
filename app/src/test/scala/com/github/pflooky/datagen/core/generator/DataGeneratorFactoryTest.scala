package com.github.pflooky.datagen.core.generator

import com.github.pflooky.datagen.core.model._
import com.github.pflooky.datagen.core.util.SparkSuite
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class DataGeneratorFactoryTest extends SparkSuite {

  test("Can generate data for basic step") {
    val dataGeneratorFactory = new DataGeneratorFactory()
    val schema = Schema("manual", Some(
      List(
        Field("id", "string", Generator("random", Map())),
        Field("amount", "double", Generator("random", Map())),
        Field("name", "string", Generator("regex", Map("regex" -> "[A-Z]{1}[a-z]{4,8}"))),
        Field("debit_credit", "string", Generator("oneOf", Map("oneOf" -> List("D", "C"))))
      )
    ))
    val step = Step("transaction", "parquet", Count(total = 10), Map("path" -> "sample/output/parquet/transactions"), schema)

    val df = dataGeneratorFactory.generateDataForStep(step, "parquet")

    assert(df.count() == 10L)
    assert(df.columns sameElements Array("id", "amount", "name", "debit_credit"))
    assert(df.schema == StructType(Array(
      StructField("id", StringType, false),
      StructField("amount", DoubleType, false),
      StructField("name", StringType, false),
      StructField("debit_credit", StringType, false)
    )))
    val sampleRow = df.head()
    assert(sampleRow.getString(0).nonEmpty && sampleRow.getString(0).length <= 20)
    assert(sampleRow.getDouble(1) >= 0.0 && sampleRow.getDouble(1) <= 1.0)
    assert(sampleRow.getString(2).nonEmpty)
    assert(sampleRow.getString(2) matches "[A-Z]{1}[a-z]{4,8}")
    assert(sampleRow.getString(3) == "D" || sampleRow.getString(3) == "C")
  }

}
