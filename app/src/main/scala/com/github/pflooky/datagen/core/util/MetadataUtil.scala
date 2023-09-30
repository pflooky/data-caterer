package com.github.pflooky.datagen.core.util

import com.github.pflooky.datacaterer.api.model.Constants._
import com.github.pflooky.datacaterer.api.model.{Field, MetadataConfig}
import com.github.pflooky.datagen.core.generator.metadata.ExpressionPredictor
import com.github.pflooky.datagen.core.generator.metadata.datasource.DataSourceMetadata
import com.github.pflooky.datagen.core.generator.metadata.datasource.database.{CassandraMetadata, ColumnMetadata, MysqlMetadata, PostgresMetadata}
import com.github.pflooky.datagen.core.generator.metadata.datasource.file.FileMetadata
import com.github.pflooky.datagen.core.generator.metadata.datasource.http.HttpMetadata
import com.github.pflooky.datagen.core.generator.metadata.datasource.jms.JmsMetadata
import com.github.pflooky.datagen.core.generator.metadata.datasource.openlineage.OpenLineageMetadata
import com.github.pflooky.datagen.core.model.FieldHelper
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat
import org.apache.spark.sql.execution.command.AnalyzeColumnCommand
import org.apache.spark.sql.types.{BinaryType, BooleanType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, Metadata, MetadataBuilder, ShortType, StringType, StructField, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}

import scala.util.{Failure, Success, Try}

object MetadataUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OBJECT_MAPPER = ObjectMapperUtil.jsonObjectMapper
  private val mapStringToAnyClass = Map[String, Any]()
  private val TEMP_CACHED_TABLE_NAME = "__temp_table"
  implicit private val columnMetadataEncoder: Encoder[ColumnMetadata] = Encoders.kryo[ColumnMetadata]

  def metadataToMap(metadata: Metadata): Map[String, Any] = {
    OBJECT_MAPPER.readValue(metadata.json, mapStringToAnyClass.getClass)
  }

  def mapToMetadata(mapMetadata: Map[String, Any]): Metadata = {
    Metadata.fromJson(OBJECT_MAPPER.writeValueAsString(mapMetadata))
  }

  def mapToStructFields(sourceData: DataFrame, dataSourceReadOptions: Map[String, String],
                        columnDataProfilingMetadata: List[DataProfilingMetadata])(implicit sparkSession: SparkSession): Array[StructField] = {
    mapToStructFields(sourceData, dataSourceReadOptions, columnDataProfilingMetadata, sparkSession.emptyDataset[ColumnMetadata])
  }

  def mapToStructFields(sourceData: DataFrame, dataSourceReadOptions: Map[String, String],
                        columnDataProfilingMetadata: List[DataProfilingMetadata],
                        additionalColumnMetadata: Dataset[ColumnMetadata])(implicit sparkSession: SparkSession): Array[StructField] = {
    if (!additionalColumnMetadata.storageLevel.useMemory) additionalColumnMetadata.cache()
    val dataSourceAdditionalColumnMetadata = additionalColumnMetadata.filter(_.dataSourceReadOptions.equals(dataSourceReadOptions))
    val primaryKeys = dataSourceAdditionalColumnMetadata.filter(_.metadata.get(IS_PRIMARY_KEY).exists(_.toBoolean))
    val primaryKeyCount = primaryKeys.count()

    val fieldsWithMetadata = sourceData.schema.fields.map(field => {
      val baseMetadata = new MetadataBuilder().withMetadata(field.metadata)
      columnDataProfilingMetadata.find(_.columnName == field.name).foreach(c => baseMetadata.withMetadata(mapToMetadata(c.metadata)))

      var nullable = field.nullable
      val optFieldAdditionalMetadata = dataSourceAdditionalColumnMetadata.filter(_.column == field.name)
      if (!optFieldAdditionalMetadata.isEmpty) {
        val fieldAdditionalMetadata = optFieldAdditionalMetadata.first()
        fieldAdditionalMetadata.metadata.foreach(m => {
          if (m._1.equals(IS_NULLABLE)) nullable = m._2.toBoolean
          baseMetadata.putString(m._1, String.valueOf(m._2))
        })
        val isPrimaryKey = fieldAdditionalMetadata.metadata.get(IS_PRIMARY_KEY).exists(_.toBoolean)
        if (isPrimaryKey && primaryKeyCount == 1) {
          baseMetadata.putString(IS_UNIQUE, "true")
        }
      }
      StructField(field.name, field.dataType, nullable, baseMetadata.build())
    })

    if (sparkSession.catalog.tableExists(TEMP_CACHED_TABLE_NAME)) {
      sparkSession.catalog.uncacheTable(TEMP_CACHED_TABLE_NAME)
    }
    fieldsWithMetadata
  }

  def mapToStructFields(additionalColumnMetadata: Dataset[ColumnMetadata], dataSourceReadOptions: Map[String, String]): Array[StructField] = {
    val colsMetadata = additionalColumnMetadata.collect()
    val matchedColMetadata = colsMetadata.filter(_.dataSourceReadOptions.getOrElse(METADATA_IDENTIFIER, "").equals(dataSourceReadOptions(METADATA_IDENTIFIER)))
    matchedColMetadata.map(colMetadata => {
      val dataType = DataType.fromDDL(colMetadata.metadata(FIELD_DATA_TYPE))
      val nullable = colMetadata.metadata.get(IS_NULLABLE).map(_.toBoolean).getOrElse(DEFAULT_FIELD_NULLABLE)
      val metadata = mapToMetadata(colMetadata.metadata)
      StructField(colMetadata.column, dataType, nullable, metadata)
    })
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
      val optionalMetadataMap = Map(EXPRESSION -> optFakerExpression, ONE_OF_GENERATOR -> optOneOfColumn)
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
    if (!sparkSession.catalog.isCached(TEMP_CACHED_TABLE_NAME)) sparkSession.catalog.cacheTable(TEMP_CACHED_TABLE_NAME)
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
      .map(kv => {
        val baseStatName = kv._1.replaceFirst("col\\.", "")
        if (baseStatName.equalsIgnoreCase("minvalue")) {
          ("min", kv._2)
        } else if (baseStatName.equalsIgnoreCase("maxvalue")) {
          ("max", kv._2)
        } else (baseStatName, kv._2)
      })
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
      //      case HTTP =>
      //        //TODO do we support delete via HTTP?
      //        options(HTTP_METHOD)
      case _ =>
        LOGGER.warn(s"Unsupported data format for record tracking, format=$lowerFormat")
        throw new RuntimeException(s"Unsupported data format for record tracking, format=$lowerFormat")
    }
    s"$recordTrackingFolderPath/$lowerFormat/$dataSourceName/$subPath"
  }

  def getMetadataFromConnectionConfig(connectionConfig: (String, Map[String, String])): Option[DataSourceMetadata] = {
    val optExternalMetadataSource = connectionConfig._2.get(METADATA_SOURCE_TYPE)
    val format = connectionConfig._2(FORMAT).toLowerCase
    (optExternalMetadataSource, format) match {
      case (Some(metadataSourceType), _) =>
        metadataSourceType.toLowerCase match {
          case MARQUEZ => Some(OpenLineageMetadata(connectionConfig._1, format, connectionConfig._2))
          case metadataSourceType =>
            LOGGER.warn(s"Unsupported external metadata source, connection-name=${connectionConfig._1}, metadata-source-type=$metadataSourceType")
            None
        }
      case (_, CASSANDRA) => Some(CassandraMetadata(connectionConfig._1, connectionConfig._2))
      case (_, JDBC) =>
        connectionConfig._2(DRIVER) match {
          case POSTGRES_DRIVER => Some(PostgresMetadata(connectionConfig._1, connectionConfig._2))
          case MYSQL_DRIVER => Some(MysqlMetadata(connectionConfig._1, connectionConfig._2))
          case driver =>
            LOGGER.warn(s"Metadata extraction not supported for JDBC driver type '$driver', connection-name=${connectionConfig._1}")
            None
        }
      case (_, CSV | JSON | PARQUET | DELTA | ORC) => Some(FileMetadata(connectionConfig._1, format, connectionConfig._2))
      case (_, HTTP) => Some(HttpMetadata(connectionConfig._1, format, connectionConfig._2))
      case (_, JMS) => Some(JmsMetadata(connectionConfig._1, format, connectionConfig._2))
      case _ =>
        LOGGER.warn(s"Metadata extraction not supported for connection type '${connectionConfig._2(FORMAT)}', connection-name=${connectionConfig._1}")
        None
    }
  }

  def getFieldMetadata(dataSourceName: String, df: DataFrame, connectionConfig: Map[String, String], metadataConfig: MetadataConfig)(implicit sparkSession: SparkSession): Array[Field] = {
    val dataSourceMetadata = getMetadataFromConnectionConfig((dataSourceName, connectionConfig)).get
    val fieldMetadata = getFieldDataProfilingMetadata(df, connectionConfig, dataSourceMetadata, metadataConfig)
    val structFields = mapToStructFields(df, connectionConfig, fieldMetadata)
    structFields.map(FieldHelper.fromStructField)
  }
}


case class DataProfilingMetadata(columnName: String, metadata: Map[String, Any])
