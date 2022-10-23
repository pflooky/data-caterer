package com.github.pflooky.datagen.core.util

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

trait SparkSuite extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  implicit lazy val sparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("spark tests")
      .getOrCreate()
  }

  override protected def beforeAll(): Unit = {
    sparkSession
  }

  override protected def afterAll(): Unit = {
    sparkSession.close()
  }

  override protected def afterEach(): Unit = {
    sparkSession.catalog.clearCache()
  }
}
