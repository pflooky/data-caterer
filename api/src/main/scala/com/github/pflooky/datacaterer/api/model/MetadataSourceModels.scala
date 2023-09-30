package com.github.pflooky.datacaterer.api.model

import com.github.pflooky.datacaterer.api.model.Constants.{MARQUEZ, METADATA_SOURCE_HAS_OPEN_LINEAGE_SUPPORT, METADATA_SOURCE_TYPE}

trait MetadataSource {

  val `type`: String
  val hasOpenLineageSupport: Boolean
  val connectionOptions: Map[String, String] = Map()

  def allOptions: Map[String, String] = {
    connectionOptions ++ Map(
      METADATA_SOURCE_TYPE -> `type`,
      METADATA_SOURCE_HAS_OPEN_LINEAGE_SUPPORT -> hasOpenLineageSupport.toString
    )
  }
}

case class MarquezMetadataSource(override val connectionOptions: Map[String, String] = Map()) extends MetadataSource {

  override val `type`: String = MARQUEZ

  override val hasOpenLineageSupport: Boolean = true
}
