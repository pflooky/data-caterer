package com.github.pflooky.datacaterer.api.model

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
  lazy val LIST_MINIMUM_LENGTH = "listMinLen"
  lazy val MAXIMUM_LENGTH = "maxLen"
  lazy val LIST_MAXIMUM_LENGTH = "listMaxLen"
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
}

object MetadataOption extends Enumeration {
  protected case class MetadataConfig(confValue: String, description: String) extends super.Val

  val RandomSeed = MetadataConfig("seed", "Seed used to control what random values are generated")
  val EnableNull = MetadataConfig("enableNull", "Allow for null values to be generated")
  val ProbabilityOfNull = MetadataConfig("nullProb", "Probability between 0.0 and 1.0 for null value to be generated")
  val EnableEdgeCase = MetadataConfig("enableEdgeCase", "Allow for edge case values to be generated (see https://pflooky.github.io/data-caterer-docs/setup/generator/generator/#data-types to check edge cases for each data type)")
  val ProbabilityOfEdgeCase = MetadataConfig("edgeCaseProb", "Probability between 0.0 and 1.0 for edge case calue to be generated")
  val Minimum = MetadataConfig("min", "Minimum value to be generated")
  val Maximum = MetadataConfig("max", "Maximum value to be generated")
  val MinimumLength = MetadataConfig("minLen", "Minimum length of value to be generated")
  val MaximumLength = MetadataConfig("maxLen", "Maximum length of value to be generated")
  val AverageLength = MetadataConfig("avgLen", "Average length of value to be generated")
  val ListMinimumLength = MetadataConfig("listMinLen", "Minimum length of list of values to be generated")
  val ListMaximumLength = MetadataConfig("listMaxLen", "Maximum length of list of values to be generated")
  val ArrayType = MetadataConfig("arrayType", "Data type used within array structure")
  val Expression = MetadataConfig("expression", "Datafaker expression to be used to generate data (see https://www.datafaker.net/documentation/expressions/)")
  val IsPrimaryKey = MetadataConfig("isPrimaryKey", "Field is part of the primary key or not")
  val PrimaryKeyPosition = MetadataConfig("primaryKeyPos", "Position of the field within the primary key. Set to -1 if not part of the primary keys")
  val ClusteringPosition = MetadataConfig("clusteringPos", "Position of the field within the clustering keys (if data source supports clustering keys). Set to -1 if not part of the clustering keys")
  val IsUnique = MetadataConfig("isUnique", "Field values generated should be unique within the session of generating values. Will not check against existing data in data source for uniqueness (expensive operation, support may be added later)")
  val NumericPrecision = MetadataConfig("precision", "Precision of numeric value")
  val NumericScale = MetadataConfig("scale", "Scale of numeric value")
  val Omit = MetadataConfig("omit", "Remove the field from the final output. Can be used as an intermediate step during data generation")
}