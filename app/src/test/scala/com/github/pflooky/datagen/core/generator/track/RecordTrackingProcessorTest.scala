package com.github.pflooky.datagen.core.generator.track

import com.github.pflooky.datagen.core.model.Constants.{IS_PRIMARY_KEY, PRIMARY_KEY_POSITION}
import com.github.pflooky.datagen.core.model.{Count, Field, Generator, Schema, Step}
import com.github.pflooky.datagen.core.util.SparkSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RecordTrackingProcessorTest extends SparkSuite {

  test("Can get all primary keys in order") {
    val schema = Schema("manual", Some(List(
      Field("name", generator = Some(
        Generator("random", Map(
          IS_PRIMARY_KEY -> "true", PRIMARY_KEY_POSITION -> "2"
        )))),
      Field("account_id", generator = Some(
        Generator("random", Map(
          IS_PRIMARY_KEY -> "true", PRIMARY_KEY_POSITION -> "1"
        )))),
      Field("balance", generator = Some(
        Generator("random", Map(
          IS_PRIMARY_KEY -> "false"
        ))))
    )))
    val step = Step("create accounts", "jdbc", Count(), Map(), schema)
    val primaryKeys = step.getPrimaryKeys
    assert(primaryKeys == List("account_id", "name"))
  }
}
