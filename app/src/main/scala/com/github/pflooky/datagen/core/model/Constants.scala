package com.github.pflooky.datagen.core.model

object Constants {

  //base config
  lazy val BASE_FOLDER_PATH = "baseFolderPath"
  lazy val PLAN_FILE_PATH = "planFilePath"
  lazy val TASK_FOLDER_PATH = "taskFolderPath"
  lazy val ENABLE_COUNT = "enableCount"
  lazy val ENABLE_GENERATE_DATA = "enableGenerateData"
  lazy val ENABLE_GENERATE_PLAN_AND_TASKS = "enableGeneratePlanAndTasks"
  lazy val ENABLE_RECORD_TRACKING = "enableRecordTracking"
  lazy val RECORD_TRACKING_FOLDER_PATH = "recordTrackingFolderPath"
  lazy val ENABLE_DELETE_GENERATED_RECORDS = "enableDeleteGeneratedRecords"
  lazy val SPARK_MASTER = "spark.master"

  //spark data options
  lazy val FORMAT = "format"
  lazy val PATH = "path"
  lazy val SAVE_MODE = "saveMode"
  lazy val CASSANDRA_KEYSPACE = "keyspace"
  lazy val CASSANDRA_TABLE = "table"
  lazy val JDBC_TABLE = "dbtable"
  lazy val JDBC_QUERY = "query"
  lazy val URL = "url"
  lazy val USERNAME = "user"
  lazy val PASSWORD = "password"
  lazy val DRIVER = "driver"
  lazy val BODY_FIELD = "bodyField"
  lazy val HTTP_METHOD = "httpMethod"
  lazy val HTTP_HEADER_PREFIX = "httpHeader"
  lazy val HTTP_CONTENT_TYPE = "httpContentType"
  lazy val JMS_DESTINATION_NAME = "destinationName"
  lazy val JMS_INITIAL_CONTEXT_FACTORY = "initialContextFactory"
  lazy val JMS_VPN_NAME = "vpnName"

  //custom spark options
  lazy val METADATA_FILTER_SCHEMA = "filterSchema"
  lazy val METADATA_FILTER_TABLE = "filterTable"

  //supported data formats
  lazy val CASSANDRA = "org.apache.spark.sql.cassandra"
  lazy val JDBC = "jdbc"
  lazy val POSTGRES = "postgres"
  lazy val HTTP = "http"
  lazy val JMS = "jms"
  //file formats
  lazy val CSV = "csv"
  lazy val DELTA = "delta"
  lazy val JSON = "json"
  lazy val PARQUET = "parquet"
  lazy val XML = "xml"
  lazy val SUPPORTED_CONNECTION_FORMATS: List[String] = List(CSV, JSON, PARQUET, CASSANDRA, JDBC)

  //supported jdbc drivers
  lazy val POSTGRES_DRIVER = "org.postgresql.Driver"
  lazy val MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

  //generator types
  lazy val RANDOM = "random"
  lazy val ONE_OF = "oneOf"
  lazy val REGEX = "regex"

  //per column generator
  lazy val PER_COLUMN_COUNT = "_per_col_count"
  lazy val PER_COLUMN_INDEX_COL = "_per_col_index"
  lazy val RECORD_COUNT_GENERATOR_COL = "record_count_generator"

  //field metadata
  lazy val RANDOM_SEED = "seed"
  lazy val ENABLED_NULL = "enableNull"
  lazy val ENABLED_EDGE_CASES = "enableEdgeCases"
  lazy val AVERAGE_LENGTH = "avgLen"
  lazy val MINIMUM_LENGTH = "minLen"
  lazy val LIST_MINIMUM_LENGTH = "listMinLen"
  lazy val MAXIMUM_LENGTH = "maxLen"
  lazy val LIST_MAXIMUM_LENGTH = "listMaxLen"
  lazy val SOURCE_MAXIMUM_LENGTH = "sourceMaxLen"
  lazy val MINIMUM_VALUE = "min"
  lazy val MAXIMUM_VALUE = "max"
  lazy val ARRAY_TYPE = "arrayType"
  lazy val EXPRESSION = "expression"
  lazy val DISTINCT_COUNT = "distinctCount"
  lazy val IS_PRIMARY_KEY = "isPrimaryKey"
  lazy val PRIMARY_KEY_POSITION = "primaryKeyPosition"
  lazy val IS_UNIQUE = "isUnique"
  lazy val IS_NULLABLE = "isNullable"
  lazy val NULL_COUNT = "nullCount"
  lazy val HISTOGRAM = "histogram"
  lazy val SOURCE_COLUMN_DATA_TYPE = "sourceDataType"
  lazy val NUMERIC_PRECISION = "numericPrecision"
  lazy val NUMERIC_SCALE = "numericScale"
  lazy val DEFAULT_VALUE = "defaultValue"
  lazy val CONSTRAINT_TYPE = "constraintType"

  //one of generator types
  lazy val ONE_OF_STRING = "string"
  lazy val ONE_OF_LONG = "long"
  lazy val ONE_OF_DOUBLE = "double"
  lazy val ONE_OF_BOOLEAN = "boolean"

  //schema type
  lazy val MANUAL = "manual"
  lazy val GENERATED = "generated"

  //status
  lazy val STARTED = "started"
  lazy val FINISHED = "finished"
  lazy val FAILED = "failed"

  //misc
  lazy val NESTED_FIELD_NAME_DELIMITER = "||"
  lazy val ARRAY_NESTED_FIELD_NAME_DELIMITER = ">>"
  lazy val APPLICATION_CONFIG_PATH = "APPLICATION_CONFIG_PATH"
  lazy val BATCH = "batch"
  lazy val REAL_TIME = "real-time"
}
