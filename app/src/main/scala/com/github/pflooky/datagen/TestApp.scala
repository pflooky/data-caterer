package com.github.pflooky.datagen

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.typedlit

object TestApp extends App {

  case class TestData(id: String, name: String)

  val spark = SparkSession.builder().appName("test").master("local[4]").getOrCreate()

  val df = spark.createDataFrame(List(
    TestData("id123", "peter"),
    TestData("id124", "cindy"),
    TestData("id125", "bob"),
    TestData("id126", "jane"),
  ))
  df.show()
  val list = (1 to 10).flatMap(i => Seq(i, s"abc${i}")).toArray

  df.withColumn("other_cols", typedlit(list))
    .show()
}
