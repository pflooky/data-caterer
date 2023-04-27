package com.github.pflooky.datagen.core.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.pflooky.datagen.core.generator.plan.datasource.DataSourceMetadata
import com.github.pflooky.datagen.core.generator.plan.datasource.database.ColumnMetadata
import com.github.pflooky.datagen.core.model.Constants.{HISTOGRAM, IS_NULLABLE}
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.AnalyzeColumnCommand
import org.apache.spark.sql.types.{BinaryType, BooleanType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, Metadata, MetadataBuilder, ShortType, StringType, StructField, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.{Failure, Success, Try}

object MetadataUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OBJECT_MAPPER = new ObjectMapper()
  OBJECT_MAPPER.registerModule(DefaultScalaModule)
  private val mapStringToAnyClass = Map[String, Any]()
  private val TEMP_CACHED_TABLE_NAME = "temp_table"

  def toMap(metadata: Metadata): Map[String, Any] = {
    OBJECT_MAPPER.readValue(metadata.json, mapStringToAnyClass.getClass)
  }

  def mapToStructFields(sparkSession: SparkSession, sourceData: DataFrame, dataSourceReadOptions: Map[String, String],
                        columnDataProfilingMetadata: Map[String, Map[String, String]],
                        additionalColumnMetadata: Dataset[ColumnMetadata]): Array[StructField] = {
    val fieldsWithMetadata = sourceData.schema.fields.map(field => {
      val baseMetadata = new MetadataBuilder().withMetadata(field.metadata)
      if (columnDataProfilingMetadata.contains(field.name)) {
        columnDataProfilingMetadata(field.name)
          .foreach(s => baseMetadata.putString(s._1.replace(s"${field.name}.", ""), s._2))
      }

      var nullable = field.nullable
      val optFieldAdditionalMetadata = additionalColumnMetadata.filter(c => c.dataSourceReadOptions.equals(dataSourceReadOptions) && c.column == field.name)
      if (!optFieldAdditionalMetadata.isEmpty) {
        val fieldAdditionalMetadata = optFieldAdditionalMetadata.first()
        fieldAdditionalMetadata.metadata.foreach(m => {
          if (m._1.equals(IS_NULLABLE)) {
            nullable = m._2.toBoolean
          }
          baseMetadata.putString(m._1, String.valueOf(m._2))
        })
      }
      StructField(field.name, field.dataType, nullable, baseMetadata.build())
    })

    sparkSession.catalog.uncacheTable(TEMP_CACHED_TABLE_NAME)
    fieldsWithMetadata
  }

  def getFieldDataProfilingMetadata(sparkSession: SparkSession, sourceData: DataFrame, dataSourceReadOptions: Map[String, String],
                                    dataSourceMetadata: DataSourceMetadata): Map[String, Map[String, String]] = {
    computeColumnStatistics(sparkSession, sourceData, dataSourceReadOptions, dataSourceMetadata.name, dataSourceMetadata.format)
    val columnLevelStatistics = sparkSession.sharedState.cacheManager.lookupCachedData(sourceData).get.cachedRepresentation.stats
    LOGGER.info(s"Computed metadata statistics for data source, name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}, " +
      s"details=$dataSourceReadOptions, rows-analysed=${columnLevelStatistics.rowCount.getOrElse(BigInt(0))}, size-in-bytes=${columnLevelStatistics.sizeInBytes}, " +
      s"num-columns-analysed=${columnLevelStatistics.attributeStats.size}")

    columnLevelStatistics.attributeStats.map(x => {
      val columnName = x._1.name
      val statisticsMap = x._2.toCatalogColumnStat(columnName, x._1.dataType).toMap(columnName)
      LOGGER.debug(s"Column summary statistics, name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}, column-name=$columnName, " +
        s"statistics=${statisticsMap - s"$columnName.$HISTOGRAM"}")
      (columnName, statisticsMap)
    })
  }

  private def computeColumnStatistics(sparkSession: SparkSession, sourceData: DataFrame, dataSourceReadOptions: Map[String, String],
                                      dataSourceName: String, dataSourceFormat: String): Unit = {
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
        LOGGER.debug(s"Successfully analyzed all columns in data source, name=$dataSourceName, format=$dataSourceFormat, options=$dataSourceReadOptions")
    }
  }

  private def analyzeSupportsType(dataType: DataType): Boolean = dataType match {
    case IntegerType | ShortType | LongType | DecimalType() | DoubleType | FloatType => true
    case BooleanType => true
    case BinaryType | StringType => true
    case TimestampType | DateType => true
    case _ => false
  }
}
