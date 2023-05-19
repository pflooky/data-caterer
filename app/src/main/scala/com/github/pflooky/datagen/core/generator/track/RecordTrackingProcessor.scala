package com.github.pflooky.datagen.core.generator.track

import com.github.pflooky.datagen.core.model.Constants.{CASSANDRA, CASSANDRA_KEYSPACE, CASSANDRA_TABLE, CSV, DELTA, IS_PRIMARY_KEY, JDBC, JDBC_TABLE, JSON, PARQUET, PATH, PRIMARY_KEY_POSITION}
import com.github.pflooky.datagen.core.model.Step
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class RecordTrackingProcessor(recordTrackingFolderPath: String)(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def trackRecords(df: DataFrame, dataSourceName: String, format: String, step: Step): Unit = {
    LOGGER.info(s"Generated record tracking is enabled, data-source-name=$dataSourceName")
    //TODO: get information about primary keys so that only the primary keys need to be tracked
    //track all columns by default
    val primaryKeys = getPrimaryKeys(step)
    val columnsToTrack = if (primaryKeys.isEmpty) df.columns.toList else primaryKeys
    val subDataSourcePath = getSubDataSourcePath(dataSourceName, format, step.options)
    df.selectExpr(columnsToTrack: _*)
      .write.format(PARQUET)
      .mode(SaveMode.Append)
      .option(PATH, subDataSourcePath)
      .save()
  }

  def deleteRecords(dataSourceName: String, format: String, options: Map[String, String], connectionConfig: Map[String, String]): Unit = {
    val subDataSourcePath = getSubDataSourcePath(dataSourceName, format, options)
    val deleteRecordService = format.toLowerCase match {
      case JDBC => new JdbcDeleteRecordService
      case CASSANDRA => new CassandraDeleteRecordService
    }
    LOGGER.warn(s"Delete generated records is enabled. If 'enableRecordTracking' has been enabled, all generated records for this data source will be deleted, " +
      s"data-source-name=$dataSourceName, format=$format, details=$options")
    deleteRecordService.deleteRecords(dataSourceName, subDataSourcePath, options ++ connectionConfig)
  }

  private def getSubDataSourcePath(dataSourceName: String, format: String, options: Map[String, String]): String = {
    val lowerFormat = format.toLowerCase
    val subPath = lowerFormat match {
      case JDBC =>
        val spt = options(JDBC_TABLE).split("\\.")
        val (schema, table) = (spt.head, spt.last)
        s"$schema/$table"
      case CASSANDRA =>
        s"${options(CASSANDRA_KEYSPACE)}/${options(CASSANDRA_TABLE)}"
      case PARQUET | CSV | JSON | DELTA =>
        options(PATH).replaceAll("s3(a|n?)://|wasb(s?)://|gs://|file://|hdfs://[a-zA-Z0-9]+:[0-9]+", "")
    }
    s"$recordTrackingFolderPath/$lowerFormat/$dataSourceName/$subPath"
  }

  def getPrimaryKeys(step: Step): List[String] = {
    if (step.schema.fields.isDefined) {
      val fields = step.schema.fields.get
      fields.filter(field => {
        if (field.generator.isDefined) {
          val metadata = field.generator.get.options
          metadata.contains(IS_PRIMARY_KEY) && metadata(IS_PRIMARY_KEY).toString.toBoolean
        } else false
      })
        .map(field => (field.name, field.generator.get.options(PRIMARY_KEY_POSITION).toString.toInt))
        .sortBy(_._2)
        .map(_._1)
    } else List()
  }
}
