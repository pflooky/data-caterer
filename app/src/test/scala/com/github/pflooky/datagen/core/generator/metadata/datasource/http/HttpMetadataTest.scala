package com.github.pflooky.datagen.core.generator.metadata.datasource.http

import com.github.pflooky.datacaterer.api.model.Constants.SCHEMA_LOCATION
import com.github.pflooky.datagen.core.util.SparkSuite

class HttpMetadataTest extends SparkSuite {

  test("Can get all endpoints and schemas from OpenApi spec") {
    val result = HttpMetadata("my_http", "http", Map(SCHEMA_LOCATION -> "app/src/test/resources/sample/http/openapi/petstore.json"))
      .getSubDataSourcesMetadata

    assert(result.length == 2)
  }

}
