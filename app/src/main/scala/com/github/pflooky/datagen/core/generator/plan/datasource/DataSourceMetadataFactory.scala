package com.github.pflooky.datagen.core.generator.plan.datasource

import com.github.pflooky.datagen.core.generator.plan.PlanGenerator.writePlanAndTasksToFiles
import com.github.pflooky.datagen.core.generator.plan.datasource.database.{CassandraMetadata, DatabaseMetadata, DatabaseMetadataProcessor, MysqlMetadata, PostgresMetadata}
import com.github.pflooky.datagen.core.generator.plan.datasource.file.{FileMetadata, FileMetadataProcessor}
import com.github.pflooky.datagen.core.model.Constants.{ADVANCED_APPLICATION, BASIC_APPLICATION, CASSANDRA, CSV, DATA_CATERER_SITE_PRICING, DELTA, DRIVER, FORMAT, JDBC, JSON, MYSQL_DRIVER, ORC, PARQUET, POSTGRES_DRIVER}
import com.github.pflooky.datagen.core.model.{Plan, Task}
import com.github.pflooky.datagen.core.util.{ForeignKeyUtil, MetadataUtil, SparkProvider}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType

class DataSourceMetadataFactory extends SparkProvider {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val databaseMetadataProcessor = new DatabaseMetadataProcessor(sparkSession)
  private val fileMetadataProcessor = new FileMetadataProcessor

  def extractAllDataSourceMetadata(): Option[(Plan, List[Task])] = {
    if (applicationType.equalsIgnoreCase(ADVANCED_APPLICATION) && flagsConfig.enableGeneratePlanAndTasks) {
      LOGGER.info("Attempting to extract all data source metadata as defined in connection configurations in application.conf")
      val connectionMetadata = connectionConfigsByName.map(connectionConfig => {
        val connection = connectionConfig._2(FORMAT) match {
          case CASSANDRA => Some(CassandraMetadata(connectionConfig._1, connectionConfig._2))
          case JDBC =>
            connectionConfig._2(DRIVER) match {
              case POSTGRES_DRIVER => Some(PostgresMetadata(connectionConfig._1, connectionConfig._2))
              case MYSQL_DRIVER => Some(MysqlMetadata(connectionConfig._1, connectionConfig._2))
              case _ => None
            }
          case CSV | JSON | PARQUET | DELTA | ORC => Some(FileMetadata(connectionConfig._1, connectionConfig._2(FORMAT), connectionConfig._2))
          case _ => None
        }
        if (connection.isEmpty) {
          LOGGER.warn(s"Metadata extraction not supported for connection type '${connectionConfig._2(FORMAT)}', connection-name=${connectionConfig._1}")
        }
        connection
      }).toList

      val metadataPerConnection = connectionMetadata.filter(_.isDefined).map(_.get).map(x => {
        //given foreign keys defined for a data source, ensure these columns are always generated (OMIT -> false)
//        val foreignKeys = x.getForeignKeys
//        val metadata = getMetadataForDataSource(x)
//        metadata.filter(y => y.dataSourceMetadata.name)
        (x, x.getForeignKeys, getMetadataForDataSource(x))
      })
      val generatedTasksFromMetadata = metadataPerConnection.map(m => (m._1.name, Task.fromMetadata(m._1.name, m._1.format, m._3)))
      //given all the foreign key relations in each data source, detect if there are any links between data sources, then pass that into plan
      val allForeignKeys = ForeignKeyUtil.getAllForeignKeyRelationships(metadataPerConnection.map(_._2))

      Some(writePlanAndTasksToFiles(generatedTasksFromMetadata, allForeignKeys, foldersConfig.generatedPlanAndTaskFolderPath))
    } else if (applicationType.equalsIgnoreCase(BASIC_APPLICATION) && flagsConfig.enableGeneratePlanAndTasks) {
      LOGGER.warn(s"Please upgrade from the free plan to paid plan to enable plan and tasks to be generated. More details here: $DATA_CATERER_SITE_PRICING")
      None
    } else None
  }

  def getMetadataForDataSource(dataSourceMetadata: DataSourceMetadata): List[DataSourceDetail] = {
    LOGGER.info(s"Extracting out metadata from data source, name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}")
    val allDataSourceReadOptions = dataSourceMetadata match {
      case databaseMetadata: DatabaseMetadata => databaseMetadataProcessor.getAllDatabaseTables(databaseMetadata)
      case _ => fileMetadataProcessor.getAllFiles(dataSourceMetadata)
    }

    val numSubDataSources = allDataSourceReadOptions.length
    if (numSubDataSources == 0) {
      LOGGER.warn(s"Unable to find any sub data sources, name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}")
    } else {
      LOGGER.info(s"Found sub data sources, name=${dataSourceMetadata.name}, format=${dataSourceMetadata.format}, num-sub-data-sources=$numSubDataSources")
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
