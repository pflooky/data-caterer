package com.github.pflooky.datagen.core.model

object Constants {

  //base config
  val PLAN_FILE_PATH = "planFilePath"
  val TASK_FOLDER_PATH = "taskFolderPath"
  val ENABLE_COUNT = "enableCount"
  val SPARK_MASTER = "spark.master"

  //spark data options
  val FORMAT = "format"
  val SAVE_MODE = "saveMode"
  val CASSANDRA_KEYSPACE = "keyspace"
  val CASSANDRA_TABLE = "table"
  val JDBC_TABLE = "dbtable"

  //supported data formats
  val CASSANDRA = "org.apache.spark.sql.cassandra"
  val JDBC = "jdbc"
  val POSTGRES = "postgres"

  val CSV = "csv"
  val JSON = "json"
  val PARQUET = "parquet"
  val XML = "xml"

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
  val EXPRESSION = "expression"

  //one of generator types
  val ONE_OF_STRING = "string"
  val ONE_OF_LONG = "long"
  val ONE_OF_DOUBLE = "double"
  val ONE_OF_BOOLEAN = "boolean"

  //status
  val STARTED = "started"
  val FINISHED = "finished"
  val FAILED = "failed"
}
