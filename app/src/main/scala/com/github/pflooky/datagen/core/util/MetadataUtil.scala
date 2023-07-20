package com.github.pflooky.datagen.core.util

import com.github.pflooky.datagen.core.config.MetadataConfig
import com.github.pflooky.datagen.core.generator.plan.ExpressionPredictor
import com.github.pflooky.datagen.core.generator.plan.datasource.DataSourceMetadata
import com.github.pflooky.datagen.core.generator.plan.datasource.database.ColumnMetadata
import com.github.pflooky.datagen.core.model.Constants.{CASSANDRA, CASSANDRA_KEYSPACE, CASSANDRA_TABLE, CSV, DELTA, DISTINCT_COUNT, EXPRESSION, HISTOGRAM, HTTP, HTTP_METHOD, IS_NULLABLE, JDBC, JDBC_TABLE, JMS, JMS_DESTINATION_NAME, JSON, ONE_OF, ORC, PARQUET, PATH, ROW_COUNT}
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat
import org.apache.spark.sql.execution.command.AnalyzeColumnCommand
import org.apache.spark.sql.types.{BinaryType, BooleanType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, Metadata, MetadataBuilder, ShortType, StringType, StructField, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.{Failure, Success, Try}

object MetadataUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OBJECT_MAPPER = ObjectMapperUtil.jsonObjectMapper
  private val mapStringToAnyClass = Map[String, Any]()
  private val TEMP_CACHED_TABLE_NAME = "temp_table"

  def metadataToMap(metadata: Metadata): Map[String, Any] = {
    OBJECT_MAPPER.readValue(metadata.json, mapStringToAnyClass.getClass)
  }

  def mapToMetadata(mapMetadata: Map[String, Any]): Metadata = {
    Metadata.fromJson(OBJECT_MAPPER.writeValueAsString(mapMetadata))
  }

  def mapToStructFields(sourceData: DataFrame, dataSourceReadOptions: Map[String, String],
                        columnDataProfilingMetadata: List[DataProfilingMetadata],
                        additionalColumnMetadata: Dataset[ColumnMetadata])(implicit sparkSession: SparkSession): Array[StructField] = {
    additionalColumnMetadata.cache()
    val fieldsWithMetadata = sourceData.schema.fields.map(field => {
      val baseMetadata = new MetadataBuilder().withMetadata(field.metadata)
      columnDataProfilingMetadata.find(_.columnName == field.name).foreach(c => baseMetadata.withMetadata(mapToMetadata(c.metadata)))

      var nullable = field.nullable
      val optFieldAdditionalMetadata = additionalColumnMetadata.filter(c => c.dataSourceReadOptions.equals(dataSourceReadOptions) && c.column == field.name)
      if (!optFieldAdditionalMetadata.isEmpty) {
        val fieldAdditionalMetadata = optFieldAdditionalMetadata.first()
        fieldAdditionalMetadata.metadata.foreach(m => {
          if (m._1.equals(IS_NULLABLE)) nullable = m._2.toBoolean
          baseMetadata.putString(m._1, String.valueOf(m._2))
        })
      }
      StructField(field.name, field.dataType, nullable, baseMetadata.build())
    })

    if (sparkSession.catalog.tableExists(TEMP_CACHED_TABLE_NAME)) {
      sparkSession.catalog.uncacheTable(TEMP_CACHED_TABLE_NAME)
    }
    fieldsWithMetadata
  }

  def getFieldDataProfilingMetadata(sourceData: DataFrame, dataSourceReadOptions: Map[String, String],
                                    dataSourceMetadata: DataSourceMetadata, metadataConfig: MetadataConfig)(implicit sparkSession: SparkSession): List[DataProfilingMetadata] = {
    computeColumnStatistics(sourceData, dataSourceReadOptions, dataSourceMetadata.name, dataSourceMetadata.format)
    val columnLevelStatistics = sparkSession.sharedState.cacheManager.lookupCachedData(sourceData).get.cachedRepresentation.stats
    val rowCount = columnLevelStatistics.rowCount.getOrElse(BigInt(0))
    LOGGER.info(s"Computed metadata statistics for data source, name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}, " +
      s"details=$dataSourceReadOptions, rows-analysed=$rowCount, size-in-bytes=${columnLevelStatistics.sizeInBytes}, " +
      s"num-columns-analysed=${columnLevelStatistics.attributeStats.size}")

    columnLevelStatistics.attributeStats.map(x => {
      val columnName = x._1.name
      val statisticsMap = columnStatToMap(x._2.toCatalogColumnStat(columnName, x._1.dataType)) ++ Map(ROW_COUNT -> rowCount.toString)
      val optOneOfColumn = determineIfOneOfColumn(sourceData, columnName, statisticsMap, metadataConfig)
      val optFakerExpression = ExpressionPredictor.getFakerExpression(sourceData.schema.fields.find(_.name == columnName).get)
      val optionalMetadataMap = Map(EXPRESSION -> optFakerExpression, ONE_OF -> optOneOfColumn)
        .filter(_._2.isDefined)
        .map(x => (x._1, x._2.get))
      val statWithOptionalMetadata = statisticsMap ++ optionalMetadataMap

      LOGGER.debug(s"Column summary statistics, name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}, column-name=$columnName, " +
        s"statistics=${statWithOptionalMetadata - s"$columnName.$HISTOGRAM"}")
      DataProfilingMetadata(columnName, statWithOptionalMetadata)
    }).toList
  }

  private def computeColumnStatistics(sourceData: DataFrame, dataSourceReadOptions: Map[String, String],
                                      dataSourceName: String, dataSourceFormat: String)(implicit sparkSession: SparkSession): Unit = {
    //have to create temp view then analyze the column stats which can be found in the cached data
    sourceData.createOrReplaceTempView(TEMP_CACHED_TABLE_NAME)
    sparkSession.catalog.cacheTable(TEMP_CACHED_TABLE_NAME)
    val optColumnsToAnalyze = Some(sourceData.schema.fields.filter(f => analyzeSupportsType(f.dataType)).map(_.name).toSeq)
    val tryAnalyzeData = Try(AnalyzeColumnCommand(TableIdentifier(TEMP_CACHED_TABLE_NAME), optColumnsToAnalyze, false).run(sparkSession))
    tryAnalyzeData match {
      case Failure(exception) =>
        LOGGER.error(s"Failed to analyze all columns in data source, name=$dataSourceName, format=$dataSourceFormat, " +
          s"options=$dataSourceReadOptions, error-message=${exception.getMessage}")
      case Success(_) =>
        LOGGER.debug(s"Successfully analyzed all columns in data source, name=$dataSourceName, " +
          s"format=$dataSourceFormat, options=$dataSourceReadOptions")
    }
  }

  private def analyzeSupportsType(dataType: DataType): Boolean = dataType match {
    case IntegerType | ShortType | LongType | DecimalType() | DoubleType | FloatType => true
    case BooleanType => true
    case BinaryType | StringType => true
    case TimestampType | DateType => true
    case _ => false
  }

  private def columnStatToMap(catalogColumnStat: CatalogColumnStat): Map[String, String] = {
    catalogColumnStat.toMap("col")
      .map(kv => (kv._1.replaceFirst("col\\.", ""), kv._2))
      .filter(_._1 != "version")
  }

  def determineIfOneOfColumn(sourceData: DataFrame, columnName: String,
                             statisticsMap: Map[String, String], metadataConfig: MetadataConfig): Option[Array[String]] = {
    val columnDataType = sourceData.schema.fields.find(_.name == columnName).map(_.dataType)
    val count = statisticsMap(ROW_COUNT).toLong
    (columnDataType, count) match {
      case (Some(DateType), _) => None
      case (_, 0) => None
      case (Some(_), c) if c >= metadataConfig.oneOfMinCount =>
        val distinctCount = statisticsMap(DISTINCT_COUNT).toDouble
        if (distinctCount / count <= metadataConfig.oneOfDistinctCountVsCountThreshold) {
          LOGGER.debug(s"Identified column as a 'oneOf' column as distinct count / total count is below threshold, threshold=${metadataConfig.oneOfDistinctCountVsCountThreshold}")
          Some(sourceData.select(columnName).distinct().collect().map(_.mkString))
        } else {
          None
        }
      case _ => None
    }
  }

  def getSubDataSourcePath(dataSourceName: String, format: String, options: Map[String, String], recordTrackingFolderPath: String): String = {
    val lowerFormat = format.toLowerCase
    val subPath = lowerFormat match {
      case JDBC =>
        val spt = options(JDBC_TABLE).split("\\.")
        val (schema, table) = (spt.head, spt.last)
        s"$schema/$table"
      case CASSANDRA =>
        s"${options(CASSANDRA_KEYSPACE)}/${options(CASSANDRA_TABLE)}"
      case PARQUET | CSV | JSON | DELTA | ORC =>
        options(PATH).replaceAll("s3(a|n?)://|wasb(s?)://|gs://|file://|hdfs://[a-zA-Z0-9]+:[0-9]+", "")
      case JMS =>
        options(JMS_DESTINATION_NAME)
      case HTTP =>
        //TODO do we support delete via HTTP?
        options(HTTP_METHOD)
      case _ =>
        throw new RuntimeException(s"Unsupported data format for record tracking, format=$lowerFormat")
    }
    s"$recordTrackingFolderPath/$lowerFormat/$dataSourceName/$subPath"
  }
}

case class DataProfilingMetadata(columnName: String, metadata: Map[String, Any])
