package com.github.pflooky.datagen.core.model

import com.github.pflooky.datacaterer.api.model.Constants.{CASSANDRA, CSV, HTTP, JDBC, JMS, JSON, KAFKA, ORC, PARQUET}

object Constants {

  //app type
  lazy val BASIC_APPLICATION = "basic"
  lazy val ADVANCED_APPLICATION = "advanced"

  //base config
  lazy val RUNTIME_MASTER = "runtime.master"

  //spark data options
  lazy val DEFAULT_HTTP_METHOD = "GET"
  lazy val DEFAULT_HTTP_CONTENT_TYPE = "application/json"

  //custom spark options
  lazy val METADATA_FILTER_OUT_SCHEMA = "filterOutSchema"
  lazy val METADATA_FILTER_OUT_TABLE = "filterOutTable"

  //supported data formats
  lazy val SUPPORTED_CONNECTION_FORMATS: List[String] = List(CSV, JSON, ORC, PARQUET, CASSANDRA, JDBC, HTTP, JMS, KAFKA)
  lazy val BASIC_APPLICATION_SUPPORTED_CONNECTION_FORMATS: List[String] = List(CSV, JSON, ORC, PARQUET, JDBC)

  //special column names
  lazy val PER_COLUMN_COUNT = "_per_col_count"
  lazy val JOIN_FOREIGN_KEY_COL = "_join_foreign_key"
  lazy val PER_COLUMN_INDEX_COL = "_per_col_index"
  lazy val RECORD_COUNT_GENERATOR_COL = "record_count_generator"
  lazy val INDEX_INC_COL = "__index_inc"
  lazy val REAL_TIME_BODY_COL = "value"
  lazy val REAL_TIME_PARTITION_COL = "partition"
  lazy val REAL_TIME_HEADERS_COL = "headers"
  lazy val REAL_TIME_METHOD_COL = "method"
  lazy val REAL_TIME_CONTENT_TYPE_COL = "content_type"
  lazy val REAL_TIME_URL_COL = "url"

  //spark udf
  lazy val GENERATE_REGEX_UDF = "GENERATE_REGEX"
  lazy val GENERATE_FAKER_EXPRESSION_UDF = "GENERATE_FAKER_EXPRESSION"
  lazy val GENERATE_RANDOM_ALPHANUMERIC_STRING_UDF = "GENERATE_RANDOM_ALPHANUMERIC_STRING"

  //status
  lazy val STARTED = "started"
  lazy val FINISHED = "finished"
  lazy val FAILED = "failed"

  //count
  lazy val COUNT_TYPE = "countType"
  lazy val COUNT_BASIC = "basic-count"
  lazy val COUNT_GENERATED = "generated-count"
  lazy val COUNT_PER_COLUMN = "per-column-count"
  lazy val COUNT_GENERATED_PER_COLUMN = "generated-per-column-count"
  lazy val COUNT_COLUMNS = "columns"
  lazy val COUNT_NUM_RECORDS = "numRecords"

  //report
  lazy val REPORT_DATA_SOURCES_HTML = "data-sources.html"
  lazy val REPORT_FIELDS_HTML = "steps.html"
  lazy val REPORT_HOME_HTML = "index.html"
  lazy val REPORT_VALIDATIONS_HTML = "validations.html"

  //misc
  lazy val APPLICATION_CONFIG_PATH = "APPLICATION_CONFIG_PATH"
  lazy val BATCH = "batch"
  lazy val REAL_TIME = "real-time"
  lazy val DEFAULT_ROWS_PER_SECOND = "5"
  lazy val DATA_CATERER_SITE_PRICING = "https://data.catering/pricing/"

}
