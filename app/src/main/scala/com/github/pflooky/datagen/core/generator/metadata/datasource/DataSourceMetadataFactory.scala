package com.github.pflooky.datagen.core.generator.metadata.datasource

import com.github.pflooky.datacaterer.api.model.{DataCatererConfiguration, Plan, Task}
import com.github.pflooky.datagen.core.config.ConfigParser
import com.github.pflooky.datagen.core.generator.metadata.PlanGenerator.writePlanAndTasksToFiles
import com.github.pflooky.datagen.core.generator.metadata.datasource.database.{DatabaseMetadata, DatabaseMetadataProcessor}
import com.github.pflooky.datagen.core.generator.metadata.datasource.file.{FileMetadata, FileMetadataProcessor}
import com.github.pflooky.datagen.core.generator.metadata.datasource.http.{HttpMetadata, HttpMetadataProcessor}
import com.github.pflooky.datagen.core.generator.metadata.datasource.jms.{JmsMetadata, JmsMetadataProcessor}
import com.github.pflooky.datagen.core.model.Constants.{ADVANCED_APPLICATION, BASIC_APPLICATION, DATA_CATERER_SITE_PRICING}
import com.github.pflooky.datagen.core.model.TaskHelper
import com.github.pflooky.datagen.core.util.MetadataUtil.getMetadataFromConnectionConfig
import com.github.pflooky.datagen.core.util.{ForeignKeyUtil, MetadataUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

class DataSourceMetadataFactory(dataCatererConfiguration: DataCatererConfiguration)(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val metadataConfig = dataCatererConfiguration.metadataConfig
  private val flagsConfig = dataCatererConfiguration.flagsConfig
  private val applicationType = ConfigParser.applicationType

  def extractAllDataSourceMetadata(): Option[(Plan, List[Task])] = {
    if (applicationType.equalsIgnoreCase(ADVANCED_APPLICATION) && flagsConfig.enableGeneratePlanAndTasks) {
      LOGGER.info("Attempting to extract all data source metadata as defined in connection configurations in application config")
      val connectionMetadata = dataCatererConfiguration.connectionConfigByName.map(getMetadataFromConnectionConfig).filter(_.isDefined).map(_.get).toList

      val metadataPerConnection = connectionMetadata.map(x => (x, x.getForeignKeys, getMetadataForDataSource(x)))
      val generatedTasksFromMetadata = metadataPerConnection.map(m => (m._1.name, TaskHelper.fromMetadata(m._1.name, m._1.format, m._3)))
      //given all the foreign key relations in each data source, detect if there are any links between data sources, then pass that into plan
      val allForeignKeys = ForeignKeyUtil.getAllForeignKeyRelationships(metadataPerConnection.map(_._2))

      Some(writePlanAndTasksToFiles(generatedTasksFromMetadata, allForeignKeys, dataCatererConfiguration.foldersConfig.generatedPlanAndTaskFolderPath))
    } else if (applicationType.equalsIgnoreCase(BASIC_APPLICATION) && flagsConfig.enableGeneratePlanAndTasks) {
      LOGGER.warn(s"Please upgrade from the free plan to paid plan to enable plan and tasks to be generated. More details here: $DATA_CATERER_SITE_PRICING")
      None
    } else None
  }

  def getMetadataForDataSource(dataSourceMetadata: DataSourceMetadata): List[DataSourceDetail] = {
    LOGGER.info(s"Extracting out metadata from data source, name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}")
    val metadataProcessor = dataSourceMetadata match {
      case databaseMetadata: DatabaseMetadata => new DatabaseMetadataProcessor(databaseMetadata)
      case _: FileMetadata => new FileMetadataProcessor(dataSourceMetadata)
      case _: HttpMetadata => new HttpMetadataProcessor(dataSourceMetadata)
      case _: JmsMetadata => new JmsMetadataProcessor(dataSourceMetadata)
    }
    val allDataSourceReadOptions = metadataProcessor.getSubDataSourcesMetadata

    allDataSourceReadOptions.length match {
      case 0 => LOGGER.warn(s"Unable to find any sub data sources, name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}")
      case i => LOGGER.info(s"Found sub data sources, name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}, num-sub-data-sources=$i")
    }

    val additionalColumnMetadata = dataSourceMetadata.getAdditionalColumnMetadata

    allDataSourceReadOptions.map(dataSourceReadOptions => {
      LOGGER.debug(s"Reading in records from data source for metadata analysis, name=${dataSourceMetadata.name}, options=$dataSourceReadOptions, " +
        s"num-records-from-data-source=${metadataConfig.numRecordsFromDataSource}, num-records-for-analysis=${metadataConfig.numRecordsForAnalysis}")
      val data = sparkSession.read
        .format(dataSourceMetadata.format)
        .options(dataSourceMetadata.connectionConfig ++ dataSourceReadOptions)
        .load()
        .limit(metadataConfig.numRecordsFromDataSource)
        .sample(metadataConfig.numRecordsForAnalysis.toDouble / metadataConfig.numRecordsFromDataSource)

      val fieldsWithDataProfilingMetadata = MetadataUtil.getFieldDataProfilingMetadata(data, dataSourceReadOptions, dataSourceMetadata, metadataConfig)
      val structFields = MetadataUtil.mapToStructFields(data, dataSourceReadOptions, fieldsWithDataProfilingMetadata, additionalColumnMetadata)
      DataSourceDetail(dataSourceMetadata, dataSourceReadOptions, StructType(structFields))
    }).toList
  }
}

case class DataSourceDetail(dataSourceMetadata: DataSourceMetadata, sparkOptions: Map[String, String], structType: StructType)
