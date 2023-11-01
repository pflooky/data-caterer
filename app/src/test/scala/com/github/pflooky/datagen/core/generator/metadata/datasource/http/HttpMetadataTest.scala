package com.github.pflooky.datagen.core.generator.metadata.datasource.http

import com.github.pflooky.datacaterer.api.model.Constants.SCHEMA_LOCATION
import com.github.pflooky.datagen.core.util.SparkSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HttpMetadataTest extends SparkSuite {

  test("Can get all endpoints and schemas from OpenApi spec") {
    val result = HttpMetadata("my_http", "http", Map(SCHEMA_LOCATION -> "src/test/resources/sample/http/openapi/petstore.json"))
      .getSubDataSourcesMetadata

    assert(result.length == 4)
  }

}
