package com.github.pflooky.datagen.core.generator.metadata.datasource

import com.github.pflooky.datacaterer.api.model.{DataCatererConfiguration, DataSourceValidation, Plan, Task, ValidationConfiguration}
import com.github.pflooky.datacaterer.api.{PlanRun, ValidationBuilder}
import com.github.pflooky.datagen.core.config.ConfigParser
import com.github.pflooky.datagen.core.generator.metadata.PlanGenerator.writeToFiles
import com.github.pflooky.datagen.core.generator.metadata.datasource.database.{ColumnMetadata, ForeignKeyRelationship}
import com.github.pflooky.datagen.core.generator.metadata.validation.ValidationPredictor
import com.github.pflooky.datagen.core.model.Constants.{ADVANCED_APPLICATION, BASIC_APPLICATION, DATA_CATERER_SITE_PRICING, TRIAL_APPLICATION}
import com.github.pflooky.datagen.core.model.{TaskHelper, ValidationConfigurationHelper}
import com.github.pflooky.datagen.core.util.MetadataUtil.getMetadataFromConnectionConfig
import com.github.pflooky.datagen.core.util.{ForeignKeyUtil, MetadataUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

class DataSourceMetadataFactory(dataCatererConfiguration: DataCatererConfiguration)(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val metadataConfig = dataCatererConfiguration.metadataConfig
  private val flagsConfig = dataCatererConfiguration.flagsConfig
  private val applicationType = ConfigParser.applicationType

  def extractAllDataSourceMetadata(optPlanRun: Option[PlanRun]): Option[(Plan, List[Task], ValidationConfiguration)] = {
    if ((applicationType.equalsIgnoreCase(ADVANCED_APPLICATION) || applicationType.equalsIgnoreCase(TRIAL_APPLICATION)) &&
      flagsConfig.enableGeneratePlanAndTasks) {
      LOGGER.info("Attempting to extract all data source metadata as defined in connection configurations in application config")
      val connectionMetadata = dataCatererConfiguration.connectionConfigByName.map(getMetadataFromConnectionConfig).filter(_.isDefined).map(_.get).toList

      val metadataPerConnection = connectionMetadata.map(x => (x, x.getForeignKeys, getMetadataForDataSource(x)))
      val generatedTasksFromMetadata = metadataPerConnection.map(m => (m._1.name, TaskHelper.fromMetadata(optPlanRun, m._1.name, m._1.format, m._3)))
      val stepMapping = generatedTasksFromMetadata.flatMap(_._2._2)
      val generatedTasks = generatedTasksFromMetadata.map(x => (x._1, x._2._1))
      //given all the foreign key relations in each data source, detect if there are any links between data sources, then pass that into plan
      //the step name may be updated if it has come from a metadata source, need to update foreign key definitions as well with new step name
      val allForeignKeys = ForeignKeyUtil.getAllForeignKeyRelationships(metadataPerConnection.map(_._2), optPlanRun, stepMapping.toMap)
      val validationConfig = getValidationConfiguration(metadataPerConnection, optPlanRun)
      connectionMetadata.foreach(_.close())

      Some(writeToFiles(generatedTasks, allForeignKeys, validationConfig, dataCatererConfiguration.foldersConfig))
    } else if (applicationType.equalsIgnoreCase(BASIC_APPLICATION) && flagsConfig.enableGeneratePlanAndTasks) {
      LOGGER.warn(s"Please upgrade from the free plan to paid plan to enable plan and tasks to be generated. More details here: $DATA_CATERER_SITE_PRICING")
      None
    } else None
  }

  def getMetadataForDataSource(dataSourceMetadata: DataSourceMetadata): List[DataSourceDetail] = {
    LOGGER.info(s"Extracting out metadata from data source, name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}")
    val allDataSourceReadOptions = dataSourceMetadata.getSubDataSourcesMetadata
    allDataSourceReadOptions.length match {
      case 0 => LOGGER.warn(s"Unable to find any sub data sources, name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}")
      case i => LOGGER.info(s"Found sub data sources, name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}, num-sub-data-sources=$i")
    }

    val additionalColumnMetadata = dataSourceMetadata.getAdditionalColumnMetadata
    allDataSourceReadOptions.map(subDataSourceMeta => {
      val columnMetadata = subDataSourceMeta.optColumnMetadata.getOrElse(additionalColumnMetadata)
      getFieldLevelMetadata(dataSourceMetadata, columnMetadata, subDataSourceMeta.readOptions)
    }).toList
  }

  private def getFieldLevelMetadata(
                                     dataSourceMetadata: DataSourceMetadata,
                                     additionalColumnMetadata: Dataset[ColumnMetadata],
                                     dataSourceReadOptions: Map[String, String]
                                   ): DataSourceDetail = {
    if (flagsConfig.enableDeleteGeneratedRecords) {
      LOGGER.debug(s"Delete records is enabled, skipping field level metadata analysis of data source, name=${dataSourceMetadata.name}")
      DataSourceDetail(dataSourceMetadata, dataSourceReadOptions, StructType(Seq()))
    } else if (!dataSourceMetadata.hasSourceData) {
      LOGGER.debug(s"Metadata source does not contain source data for data analysis. Field level metadata will not be calculated, name=${dataSourceMetadata.name}")
      val structFields = MetadataUtil.mapToStructFields(additionalColumnMetadata, dataSourceReadOptions)
      val validations = getGeneratedValidations(dataSourceMetadata, dataSourceReadOptions, structFields)
      DataSourceDetail(dataSourceMetadata, dataSourceReadOptions, StructType(structFields), validations)
    } else {
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
      val validations = getGeneratedValidations(dataSourceMetadata, dataSourceReadOptions, structFields)
      DataSourceDetail(dataSourceMetadata, dataSourceReadOptions, StructType(structFields), validations)
    }
  }

  private def getValidationConfiguration(
                                          metadataPerConnection: List[(DataSourceMetadata, Dataset[ForeignKeyRelationship], List[DataSourceDetail])],
                                          optPlanRun: Option[PlanRun]
                                        ) = {
    val dataSourceValidations = metadataPerConnection.flatMap(m => {
      m._3.map(_.toDataSourceValidation)
    }).toMap
    val generatedValidationConfig = ValidationConfiguration(dataSources = dataSourceValidations)
    if (optPlanRun.isDefined && optPlanRun.get._validations.nonEmpty) {
      val userValidationConfig = optPlanRun.get._validations
      ValidationConfigurationHelper.merge(userValidationConfig, generatedValidationConfig)
    } else {
      generatedValidationConfig
    }
  }

  private def getGeneratedValidations(dataSourceMetadata: DataSourceMetadata, dataSourceReadOptions: Map[String, String],
                                      structFields: Array[StructField]): List[ValidationBuilder] = {
    if (flagsConfig.enableGenerateValidations) {
      LOGGER.debug("Generate validations is enabled")
      val suggestedValidations = ValidationPredictor.suggestValidations(dataSourceMetadata, dataSourceReadOptions, structFields)
      val validationsFromDataSource = dataSourceMetadata.getDataSourceValidations(dataSourceReadOptions)
      suggestedValidations ++ validationsFromDataSource
    } else {
      LOGGER.debug("Generate validations is disabled")
      List()
    }
  }
}

case class DataSourceDetail(
                             dataSourceMetadata: DataSourceMetadata,
                             sparkOptions: Map[String, String],
                             structType: StructType,
                             validations: List[ValidationBuilder] = List()
                           ) {
  def toDataSourceValidation: (String, List[DataSourceValidation]) =
    (dataSourceMetadata.name, List(DataSourceValidation(sparkOptions, validations = validations)))
}
