package com.github.pflooky.datagen.core.model

object Constants {

  //supported data formats
  val CASSANDRA = "cassandra"
  val JDBC = "jdbc"
  val POSTGRES = "postgres"
  val PARQUET = "parquet"
  val JSON = "json"
  val S3 = "postgres"

  //generator types
  val RANDOM = "random"
  val ONE_OF = "oneOf"
  val REGEX = "regex"

  //per column generator
  val PER_COLUMN_COUNT = "_per_col_count"
  val PER_COLUMN_INDEX_COL = "_per_col_index"
  val RECORD_COUNT_GENERATOR_COL = "record_count_generator"

  //field metadata
  val RANDOM_SEED = "seed"
  val ENABLED_NULL = "enableNull"
  val ENABLED_EDGE_CASES = "enableEdgeCases"
  val MINIMUM_LENGTH = "minLength"
  val MAXIMUM_LENGTH = "maxLength"
  val MINIMUM_VALUE = "minValue"
  val MAXIMUM_VALUE = "maxValue"
  val ARRAY_TYPE = "arrayType"

  //one of generator types
  val ONE_OF_STRING = "string"
  val ONE_OF_LONG = "long"
  val ONE_OF_DOUBLE = "double"
  val ONE_OF_BOOLEAN = "boolean"
}
