package com.github.pflooky.datagen.core.generator.metadata.datasource

import com.github.pflooky.datacaterer.api.model.Constants.METADATA_SOURCE_TYPE
import com.github.pflooky.datacaterer.api.model.{DataCatererConfiguration, DataSourceValidation, Plan, Schema, Task, ValidationConfiguration}
import com.github.pflooky.datacaterer.api.{PlanRun, ValidationBuilder}
import com.github.pflooky.datagen.core.config.ConfigParser
import com.github.pflooky.datagen.core.generator.metadata.PlanGenerator.writeToFiles
import com.github.pflooky.datagen.core.generator.metadata.datasource.database.{ColumnMetadata, ForeignKeyRelationship}
import com.github.pflooky.datagen.core.generator.metadata.validation.ValidationPredictor
import com.github.pflooky.datagen.core.model.Constants.{ADVANCED_APPLICATION, BASIC_APPLICATION, DATA_CATERER_SITE_PRICING, TRIAL_APPLICATION}
import com.github.pflooky.datagen.core.model.{SchemaHelper, TaskHelper, ValidationConfigurationHelper}
import com.github.pflooky.datagen.core.util.MetadataUtil.getMetadataFromConnectionConfig
import com.github.pflooky.datagen.core.util.{ConfigUtil, ForeignKeyUtil, MetadataUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.{Failure, Success, Try}

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

      //get metadata from external source if defined within optPlanRun
      val updatedPlanRun = getExternalMetadataFromPlanRun(optPlanRun)
      val metadataPerConnection = connectionMetadata.map(x => (x, x.getForeignKeys, getMetadataForDataSource(x)))
      val generatedTasksFromMetadata = metadataPerConnection.map(m => (m._1.name, TaskHelper.fromMetadata(updatedPlanRun, m._1.name, m._1.format, m._3)))
      val stepMapping = generatedTasksFromMetadata.flatMap(_._2._2)
      val generatedTasks = generatedTasksFromMetadata.map(x => (x._1, x._2._1))
      //given all the foreign key relations in each data source, detect if there are any links between data sources, then pass that into plan
      //the step name may be updated if it has come from a metadata source, need to update foreign key definitions as well with new step name
      val allForeignKeys = ForeignKeyUtil.getAllForeignKeyRelationships(metadataPerConnection.map(_._2), updatedPlanRun, stepMapping.toMap)
      val validationConfig = getValidationConfiguration(metadataPerConnection, updatedPlanRun)
      connectionMetadata.foreach(_.close())

      Some(writeToFiles(generatedTasks, allForeignKeys, validationConfig, dataCatererConfiguration.foldersConfig))
    } else if (applicationType.equalsIgnoreCase(BASIC_APPLICATION) && flagsConfig.enableGeneratePlanAndTasks) {
      LOGGER.warn(s"Please upgrade from the free plan to paid plan to enable plan and tasks to be generated. More details here: $DATA_CATERER_SITE_PRICING")
      None
    } else None
  }

  def getMetadataForDataSource(dataSourceMetadata: DataSourceMetadata): List[DataSourceDetail] = {
    LOGGER.info(s"Extracting out metadata from data source, name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}")
    val allDataSourceReadOptions = Try(dataSourceMetadata.getSubDataSourcesMetadata) match {
      case Failure(_) =>
        LOGGER.warn(s"Unable to find any sub data sources or existing metadata, name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}")
        Array[SubDataSourceMetadata]()
      case Success(value) =>
        LOGGER.info(s"Found sub data sources, name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}, num-sub-data-sources=${value.length}")
        value
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

  private def getExternalMetadataFromPlanRun(optPlanRun: Option[PlanRun]): Option[PlanRun] = {
    optPlanRun.map(planRun => {
      val updatedTaskBuilders = planRun._connectionTaskBuilders.map(connectionTaskBuilder => {
        val stepWithUpdatedSchema = connectionTaskBuilder.step.map(stepBuilder => {
          val stepSchemaFromMetadataSource = getMetadataSourceSchema(stepBuilder.step.schema)
          //remove the metadata source config from generator options after
          stepBuilder.step.copy(schema = stepSchemaFromMetadataSource)
        })
        connectionTaskBuilder.apply(connectionTaskBuilder.connectionConfigWithTaskBuilder, connectionTaskBuilder.task.map(_.task), stepWithUpdatedSchema)
      })
      planRun._connectionTaskBuilders = updatedTaskBuilders
      planRun
    })
  }

  private def getMetadataSourceSchema(userSchema: Schema): Schema = {
    val updatedFields = userSchema.fields.map(fields => {
      fields.map(f => {
        if (f.schema.isDefined) {
          val metadataForFieldSchema = Some(getMetadataSourceSchema(f.schema.get))
          f.copy(schema = metadataForFieldSchema)
        } else {
          f.generator.map(gen => {
            val isFieldDefinedFromMetadataSource = gen.options.contains(METADATA_SOURCE_TYPE)
            if (isFieldDefinedFromMetadataSource) {
              LOGGER.debug(s"Found field definition is based on metadata source, field-name=${f.name}")
            }
            val metadataSource = MetadataUtil.getMetadataFromConnectionConfig("schema_metadata" -> gen.options.map(e => e._1 -> e._2.toString))
            val fieldMetadata = metadataSource.map(getMetadataForDataSource).getOrElse(List())
            val baseDataSourceDetail = if (fieldMetadata.size > 1) {
              LOGGER.warn(s"Multiple schemas found for field level schema, will only use the first schema found from metadata source, " +
                s"metadata-source-type=${gen.options(METADATA_SOURCE_TYPE)}, num-schemas=${fieldMetadata.size}, field-name=${f.name}")
              Some(fieldMetadata.head)
            } else if (fieldMetadata.size == 1) {
              Some(fieldMetadata.head)
            } else {
              None
            }
            val updatedGeneratorOptions = ConfigUtil.cleanseOptions(gen.options.map(o => o._1 -> o._2.toString))
            val updatedGenerator = f.generator.map(gen => gen.copy(options = updatedGeneratorOptions))
            baseDataSourceDetail.map(d => {
              val metadataSourceSchema = SchemaHelper.fromStructType(d.structType)
              f.copy(schema = Some(metadataSourceSchema), generator = updatedGenerator)
            }).getOrElse(f)
          }).getOrElse(f)
        }
      })
    })
    userSchema.copy(fields = updatedFields)
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
