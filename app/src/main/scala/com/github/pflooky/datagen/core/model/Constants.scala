package com.github.pflooky.datagen.core.model

object Constants {

  //app type
  lazy val BASIC_APPLICATION = "basic"
  lazy val ADVANCED_APPLICATION = "advanced"

  //base config
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
  lazy val PARTITIONS = "partitions"
  lazy val PARTITION_BY = "partitionBy"
  lazy val BODY_FIELD = "bodyField"
  lazy val HTTP_METHOD = "httpMethod"
  lazy val HTTP_HEADER_PREFIX = "httpHeader"
  lazy val HTTP_CONTENT_TYPE = "httpContentType"
  lazy val JMS_DESTINATION_NAME = "destinationName"
  lazy val JMS_INITIAL_CONTEXT_FACTORY = "initialContextFactory"
  lazy val JMS_CONNECTION_FACTORY = "connectionFactory"
  lazy val JMS_VPN_NAME = "vpnName"
  lazy val SCHEMA_LOCATION = "schemaLocation"

  //custom spark options
  lazy val METADATA_FILTER_SCHEMA = "filterSchema"
  lazy val METADATA_FILTER_TABLE = "filterTable"

  //supported data formats
  lazy val CASSANDRA = "org.apache.spark.sql.cassandra"
  lazy val JDBC = "jdbc"
  lazy val POSTGRES = "postgres"
  lazy val HTTP = "http"
  lazy val JMS = "jms"
  lazy val KAFKA = "kafka"
  lazy val RATE = "rate"
  //file formats
  lazy val CSV = "csv"
  lazy val DELTA = "delta"
  lazy val JSON = "json"
  lazy val ORC = "orc"
  lazy val PARQUET = "parquet"
  lazy val XML = "xml"
  lazy val SUPPORTED_CONNECTION_FORMATS: List[String] = List(CSV, JSON, ORC, PARQUET, CASSANDRA, JDBC, HTTP, JMS, KAFKA)
  lazy val BASIC_APPLICATION_SUPPORTED_CONNECTION_FORMATS: List[String] = List(CSV, JSON, ORC, PARQUET, JDBC)

  //supported jdbc drivers
  lazy val POSTGRES_DRIVER = "org.postgresql.Driver"
  lazy val MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

  //generator types
  lazy val RANDOM = "random"
  lazy val ONE_OF = "oneOf"
  lazy val REGEX = "regex"
  lazy val SQL = "sql"

  //special column names
  lazy val PER_COLUMN_COUNT = "_per_col_count"
  lazy val JOIN_FOREIGN_KEY_COL = "_join_foreign_key"
  lazy val PER_COLUMN_INDEX_COL = "_per_col_index"
  lazy val RECORD_COUNT_GENERATOR_COL = "record_count_generator"
  lazy val INDEX_INC_COL = "__index_inc"

  //field metadata
  lazy val RANDOM_SEED = "seed"
  lazy val ENABLED_NULL = "enableNulls"
  lazy val PROBABILITY_OF_NULLS = "probabilityOfNulls"
  lazy val ENABLED_EDGE_CASES = "enableEdgeCases"
  lazy val PROBABILITY_OF_EDGE_CASES = "probabilityOfEdgeCases"
  lazy val AVERAGE_LENGTH = "avgLen"
  lazy val MINIMUM_LENGTH = "minLen"
  lazy val LIST_MINIMUM_LENGTH = "listMinLen"
  lazy val MAXIMUM_LENGTH = "maxLen"
  lazy val LIST_MAXIMUM_LENGTH = "listMaxLen"
  lazy val SOURCE_MAXIMUM_LENGTH = "sourceMaxLen"
  lazy val MINIMUM = "min"
  lazy val MINIMUM_VALUE = "minValue"
  lazy val MAXIMUM = "max"
  lazy val MAXIMUM_VALUE = "maxValue"
  lazy val ARRAY_TYPE = "arrayType"
  lazy val EXPRESSION = "expression"
  lazy val DISTINCT_COUNT = "distinctCount"
  lazy val ROW_COUNT = "count"
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
  lazy val DATA_SOURCE_GENERATION = "dataSourceGeneration"
  lazy val OMIT = "omit"
  lazy val CONSTRAINT_TYPE = "constraintType"

  //one of generator types
  lazy val ONE_OF_STRING = "string"
  lazy val ONE_OF_LONG = "long"
  lazy val ONE_OF_DOUBLE = "double"
  lazy val ONE_OF_BOOLEAN = "boolean"

  //schema type
  lazy val MANUAL = "manual"
  lazy val GENERATED = "generated"

  //spark udf
  lazy val GENERATE_REGEX_UDF = "GENERATE_REGEX"
  lazy val GENERATE_FAKER_EXPRESSION_UDF = "GENERATE_FAKER_EXPRESSION"
  lazy val GENERATE_RANDOM_ALPHANUMERIC_STRING_UDF = "GENERATE_RANDOM_ALPHANUMERIC_STRING"

  //status
  lazy val STARTED = "started"
  lazy val FINISHED = "finished"
  lazy val FAILED = "failed"

  //misc
  lazy val APPLICATION_CONFIG_PATH = "APPLICATION_CONFIG_PATH"
  lazy val BATCH = "batch"
  lazy val REAL_TIME = "real-time"
  lazy val DEFAULT_ROWS_PER_SECOND = "5"
  lazy val DATA_CATERER_SITE_PRICING = "https://data-catering.framer.ai/pricing"
}
