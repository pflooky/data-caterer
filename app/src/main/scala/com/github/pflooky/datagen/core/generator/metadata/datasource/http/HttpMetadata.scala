package com.github.pflooky.datagen.core.generator.metadata.datasource.http

import com.github.pflooky.datacaterer.api.model.Constants.PATH
import com.github.pflooky.datagen.core.generator.metadata.datasource.DataSourceMetadata

case class HttpMetadata(name: String, format: String, connectionConfig: Map[String, String]) extends DataSourceMetadata {

  override def toStepName(options: Map[String, String]): String = {
    options(PATH)
  }
}
