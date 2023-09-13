package com.github.pflooky.datacaterer.api.model

import java.util.UUID

object Constants {

  lazy val PLAN_CLASS = "PLAN_CLASS"

  //supported data formats
  lazy val CASSANDRA = "org.apache.spark.sql.cassandra"
  lazy val JDBC = "jdbc"
  lazy val POSTGRES = "postgres"
  lazy val MYSQL = "mysql"
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
  //jdbc drivers
  lazy val POSTGRES_DRIVER = "org.postgresql.Driver"
  lazy val MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

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
  lazy val JMS_DESTINATION_NAME = "destinationName"
  lazy val KAFKA_TOPIC = "topic"
  lazy val JMS_INITIAL_CONTEXT_FACTORY = "initialContextFactory"
  lazy val JMS_CONNECTION_FACTORY = "connectionFactory"
  lazy val JMS_VPN_NAME = "vpnName"
  lazy val SCHEMA_LOCATION = "schemaLocation"
  lazy val ROWS_PER_SECOND = "rowsPerSecond"

  //field metadata
  lazy val RANDOM_SEED = "seed"
  lazy val ENABLED_NULL = "enableNull"
  lazy val PROBABILITY_OF_NULL = "nullProb"
  lazy val ENABLED_EDGE_CASE = "enableEdgeCase"
  lazy val PROBABILITY_OF_EDGE_CASE = "edgeCaseProb"
  lazy val AVERAGE_LENGTH = "avgLen"
  lazy val MINIMUM_LENGTH = "minLen"
  lazy val ARRAY_MINIMUM_LENGTH = "arrayMinLen"
  lazy val MAXIMUM_LENGTH = "maxLen"
  lazy val ARRAY_MAXIMUM_LENGTH = "arrayMaxLen"
  lazy val SOURCE_MAXIMUM_LENGTH = "sourceMaxLen"
  lazy val MINIMUM = "min"
  lazy val MAXIMUM = "max"
  lazy val ARRAY_TYPE = "arrayType"
  lazy val EXPRESSION = "expression"
  lazy val DISTINCT_COUNT = "distinctCount"
  lazy val ROW_COUNT = "count"
  lazy val IS_PRIMARY_KEY = "isPrimaryKey"
  lazy val PRIMARY_KEY_POSITION = "primaryKeyPos"
  lazy val IS_UNIQUE = "isUnique"
  lazy val IS_NULLABLE = "isNullable"
  lazy val NULL_COUNT = "nullCount"
  lazy val HISTOGRAM = "histogram"
  lazy val SOURCE_COLUMN_DATA_TYPE = "sourceDataType"
  lazy val NUMERIC_PRECISION = "precision"
  lazy val NUMERIC_SCALE = "scale"
  lazy val DEFAULT_VALUE = "defaultValue"
  lazy val DATA_SOURCE_GENERATION = "dataSourceGeneration"
  lazy val OMIT = "omit"
  lazy val CONSTRAINT_TYPE = "constraintType"
  lazy val STATIC = "static"
  lazy val CLUSTERING_POSITION = "clusteringPos"

  //generator types
  lazy val RANDOM_GENERATOR = "random"
  lazy val ONE_OF_GENERATOR = "oneOf"
  lazy val REGEX_GENERATOR = "regex"
  lazy val SQL_GENERATOR = "sql"

  //flags defaults
  lazy val DEFAULT_ENABLE_COUNT = true
  lazy val DEFAULT_ENABLE_GENERATE_DATA = true
  lazy val DEFAULT_ENABLE_RECORD_TRACKING = false
  lazy val DEFAULT_ENABLE_DELETE_GENERATED_RECORDS = false
  lazy val DEFAULT_ENABLE_GENERATE_PLAN_AND_TASKS = false
  lazy val DEFAULT_ENABLE_FAIL_ON_ERROR = true
  lazy val DEFAULT_ENABLE_UNIQUE_CHECK = false
  lazy val DEFAULT_ENABLE_SINK_METADATA = false
  lazy val DEFAULT_ENABLE_SAVE_REPORTS = true
  lazy val DEFAULT_ENABLE_VALIDATION = false

  //folders defaults
  lazy val DEFAULT_PLAN_FILE_PATH = "/opt/app/plan/customer-create-plan.yaml"
  lazy val DEFAULT_TASK_FOLDER_PATH = "/opt/app/task"
  lazy val DEFAULT_GENERATED_PLAN_AND_TASK_FOLDER_PATH = "/tmp"
  lazy val DEFAULT_GENERATED_REPORTS_FOLDER_PATH = "/opt/app/report"
  lazy val DEFAULT_RECORD_TRACKING_FOLDER_PATH = "/opt/app/record-tracking"
  lazy val DEFAULT_VALIDATION_FOLDER_PATH = "/opt/app/validation"

  //metadata defaults
  lazy val DEFAULT_NUM_RECORD_FROM_DATA_SOURCE = 10000
  lazy val DEFAULT_NUM_RECORD_FOR_ANALYSIS = 10000
  lazy val DEFAULT_ONE_OF_DISTINCT_COUNT_VS_COUNT_THRESHOLD = 0.2
  lazy val DEFAULT_ONE_OF_MIN_COUNT = 1000
  lazy val DEFAULT_NUM_GENERATED_SAMPLES = 10

  //generation defaults
  lazy val DEFAULT_NUM_RECORDS_PER_BATCH = 100000

  //spark defaults
  lazy val DEFAULT_MASTER = "local[*]"
  lazy val DEFAULT_RUNTIME_CONFIG = java.util.Map.of(
    "spark.sql.cbo.enabled", "true",
    "spark.sql.adaptive.enabled", "true",
    "spark.sql.cbo.planStats.enabled", "true",
    "spark.sql.legacy.allowUntypedScalaUDF", "true",
    "spark.sql.statistics.histogram.enabled", "true",
    "spark.sql.shuffle.partitions", "10",
    "spark.sql.catalog.postgres", "",
    "spark.sql.catalog.cassandra", "com.datastax.spark.connector.datasource.CassandraCatalog",
    "spark.hadoop.fs.s3a.directory.marker.retention", "keep",
    "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled", "true"
  )

  //jdbc defaults
  lazy val DEFAULT_POSTGRES_URL = "jdbc:postgresql://postgresserver:5432/customer"
  lazy val DEFAULT_POSTGRES_USERNAME = "postgres"
  lazy val DEFAULT_POSTGRES_PASSWORD = "postgres"
  lazy val DEFAULT_MYSQL_URL = "jdbc:mysql://mysqlserver:3306/customer"
  lazy val DEFAULT_MYSQL_USERNAME = "root"
  lazy val DEFAULT_MYSQL_PASSWORD = "root"

  //cassandra defaults
  lazy val DEFAULT_CASSANDRA_URL = "cassandraserver:9042"
  lazy val DEFAULT_CASSANDRA_USERNAME = "cassandra"
  lazy val DEFAULT_CASSANDRA_PASSWORD = "cassandra"

  //solace defaults
  lazy val DEFAULT_SOLACE_URL = "smf://solaceserver:55554"
  lazy val DEFAULT_SOLACE_USERNAME = "admin"
  lazy val DEFAULT_SOLACE_PASSWORD = "admin"
  lazy val DEFAULT_SOLACE_VPN_NAME = "default"
  lazy val DEFAULT_SOLACE_CONNECTION_FACTORY = "/jms/cf/default"
  lazy val DEFAULT_SOLACE_INITIAL_CONTEXT_FACTORY = "com.solacesystems.jndi.SolJNDIInitialContextFactory"

  //kafka defaults
  lazy val DEFAULT_KAFKA_URL = "kafkaserver:9092"

  //foreign key defaults
  lazy val DEFAULT_FOREIGN_KEY_COLUMN = "default_column"

  //task defaults
  def DEFAULT_TASK_NAME: String = UUID.randomUUID().toString

  lazy val DEFAULT_DATA_SOURCE_NAME = "json"
  lazy val DEFAULT_TASK_SUMMARY_ENABLE = true

  //step defaults
  def DEFAULT_STEP_NAME: String = UUID.randomUUID().toString

  lazy val DEFAULT_STEP_TYPE = "json"
  lazy val DEFAULT_STEP_ENABLED = true

  //field defaults
  def DEFAULT_FIELD_NAME: String = UUID.randomUUID().toString

  lazy val DEFAULT_FIELD_TYPE = "string"
  lazy val DEFAULT_FIELD_NULLABLE = true

  //generator defaults
  lazy val DEFAULT_GENERATOR_TYPE = "random"

  //count defaults
  lazy val DEFAULT_COUNT_RECORDS = 1000L
  lazy val DEFAULT_PER_COLUMN_COUNT_RECORDS = 10L

  //validation defaults
  lazy val DEFAULT_VALIDATION_CONFIG_NAME = "default_validation"
  lazy val DEFAULT_VALIDATION_DESCRIPTION = "Validation of data sources after generating data"

}
