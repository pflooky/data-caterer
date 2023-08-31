package com.github.pflooky.datagen.core.parser

import com.github.pflooky.datacaterer.api.model.ValidationConfiguration
import org.apache.spark.sql.SparkSession

object ValidationParser {

  def parseValidation(validationFolderPath: String)(implicit sparkSession: SparkSession): Array[ValidationConfiguration] = {
    YamlFileParser.parseFiles[ValidationConfiguration](validationFolderPath)
  }
}
