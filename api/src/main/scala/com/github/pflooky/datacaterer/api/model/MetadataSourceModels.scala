package com.github.pflooky.datacaterer.api.model

import com.github.pflooky.datacaterer.api.model.Constants.{MARQUEZ, METADATA_SOURCE_HAS_OPEN_LINEAGE_SUPPORT, METADATA_SOURCE_TYPE, OPEN_API, OPEN_METADATA}

trait MetadataSource {

  val `type`: String
  val connectionOptions: Map[String, String] = Map()

  def allOptions: Map[String, String] = {
    connectionOptions ++ Map(
      METADATA_SOURCE_TYPE -> `type`
    )
  }
}

case class MarquezMetadataSource(override val connectionOptions: Map[String, String] = Map(METADATA_SOURCE_HAS_OPEN_LINEAGE_SUPPORT -> "true")) extends MetadataSource {

  override val `type`: String = MARQUEZ

}

case class OpenMetadataSource(override val connectionOptions: Map[String, String] = Map()) extends MetadataSource {

  override val `type`: String = OPEN_METADATA

}

case class OpenAPISource(override val connectionOptions: Map[String, String] = Map()) extends MetadataSource {

  override val `type`: String = OPEN_API

}
