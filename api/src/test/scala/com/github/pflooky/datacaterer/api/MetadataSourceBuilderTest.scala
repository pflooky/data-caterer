package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.model.Constants.{METADATA_SOURCE_URL, OPEN_LINEAGE_DATASET, OPEN_LINEAGE_NAMESPACE, OPEN_METADATA_API_VERSION, OPEN_METADATA_AUTH_TYPE, OPEN_METADATA_AUTH_TYPE_BASIC, OPEN_METADATA_AUTH_TYPE_OPEN_METADATA, OPEN_METADATA_BASIC_AUTH_PASSWORD, OPEN_METADATA_BASIC_AUTH_USERNAME, OPEN_METADATA_DEFAULT_API_VERSION, OPEN_METADATA_HOST, OPEN_METADATA_JWT_TOKEN, SCHEMA_LOCATION}
import com.github.pflooky.datacaterer.api.model.{MarquezMetadataSource, OpenAPISource, OpenMetadataSource}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MetadataSourceBuilderTest extends AnyFunSuite {

  test("Can create Marquez metadata source") {
    val result = MetadataSourceBuilder().marquez("localhost:8080", "food_delivery").metadataSource

    assert(result.isInstanceOf[MarquezMetadataSource])
    assert(result.asInstanceOf[MarquezMetadataSource].connectionOptions ==
      Map(METADATA_SOURCE_URL -> "localhost:8080", OPEN_LINEAGE_NAMESPACE -> "food_delivery"))
  }

  test("Can create Marquez metadata source with dataset") {
    val result = MetadataSourceBuilder().marquez("localhost:8080", "food_delivery", "public.delivery").metadataSource

    assert(result.isInstanceOf[MarquezMetadataSource])
    assert(result.asInstanceOf[MarquezMetadataSource].connectionOptions ==
      Map(METADATA_SOURCE_URL -> "localhost:8080", OPEN_LINEAGE_NAMESPACE -> "food_delivery", OPEN_LINEAGE_DATASET -> "public.delivery"))
  }

  test("Can create OpenMetadata metadata source") {
    val result = MetadataSourceBuilder().openMetadataWithToken("localhost:8080", "my_token").metadataSource

    assert(result.isInstanceOf[OpenMetadataSource])
    assert(result.asInstanceOf[OpenMetadataSource].connectionOptions ==
      Map(OPEN_METADATA_HOST -> "localhost:8080", OPEN_METADATA_API_VERSION -> OPEN_METADATA_DEFAULT_API_VERSION,
        OPEN_METADATA_AUTH_TYPE -> OPEN_METADATA_AUTH_TYPE_OPEN_METADATA, OPEN_METADATA_JWT_TOKEN -> "my_token"))
  }

  test("Can create OpenMetadata metadata source with basic auth") {
    val result = MetadataSourceBuilder().openMetadata("localhost:8080", OPEN_METADATA_AUTH_TYPE_BASIC,
      Map(OPEN_METADATA_BASIC_AUTH_USERNAME -> "username", OPEN_METADATA_BASIC_AUTH_PASSWORD -> "password")).metadataSource

    assert(result.isInstanceOf[OpenMetadataSource])
    assert(result.asInstanceOf[OpenMetadataSource].connectionOptions ==
      Map(OPEN_METADATA_HOST -> "localhost:8080", OPEN_METADATA_API_VERSION -> OPEN_METADATA_DEFAULT_API_VERSION,
        OPEN_METADATA_AUTH_TYPE -> OPEN_METADATA_AUTH_TYPE_BASIC, OPEN_METADATA_BASIC_AUTH_USERNAME -> "username",
        OPEN_METADATA_BASIC_AUTH_PASSWORD -> "password"))
  }

  test("Can create OpenAPI metadata source") {
    val result = MetadataSourceBuilder().openApi("localhost:8080").metadataSource

    assert(result.isInstanceOf[OpenAPISource])
    assert(result.asInstanceOf[OpenAPISource].connectionOptions == Map(SCHEMA_LOCATION -> "localhost:8080"))
  }

}
