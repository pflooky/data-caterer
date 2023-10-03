package com.github.pflooky.datagen.core.generator.metadata.datasource.openlineage

import com.github.pflooky.datacaterer.api.model.Constants.{DATASET_NAME, DATA_SOURCE_NAME, DEFAULT_FIELD_TYPE, DEFAULT_STEP_NAME, FACET_DATA_SOURCE, FIELD_DATA_TYPE, FIELD_DESCRIPTION, JDBC, JDBC_TABLE, METADATA_IDENTIFIER, METADATA_SOURCE_URL, OPEN_LINEAGE_DATASET, OPEN_LINEAGE_NAMESPACE, URI}
import com.github.pflooky.datagen.core.generator.metadata.datasource.DataSourceMetadata
import com.github.pflooky.datagen.core.generator.metadata.datasource.database.ColumnMetadata
import com.github.pflooky.datagen.core.model.openlineage.{ListDatasetResponse, OpenLineageDataset}
import com.github.pflooky.datagen.core.util.ObjectMapperUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.asynchttpclient.Dsl.asyncHttpClient
import org.asynchttpclient.{AsyncHttpClient, Response}

import scala.util.{Failure, Success, Try}

case class OpenLineageMetadata(
                                name: String,
                                format: String,
                                connectionConfig: Map[String, String],
                                asyncHttpClient: AsyncHttpClient = asyncHttpClient
                              ) extends DataSourceMetadata {
  require(connectionConfig.contains(METADATA_SOURCE_URL), s"Configuration missing for metadata source, metadata-source=$name, missing-configuration=$METADATA_SOURCE_URL")
  require(connectionConfig.contains(OPEN_LINEAGE_NAMESPACE), s"Configuration missing for metadata source, metadata-source=$name, missing-configuration=$OPEN_LINEAGE_NAMESPACE")

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val NAMESPACE = connectionConfig(OPEN_LINEAGE_NAMESPACE)
  private val OPT_DATASET = connectionConfig.get(OPEN_LINEAGE_DATASET)

  override val hasSourceData: Boolean = false

  override def toStepName(options: Map[String, String]): String = options.getOrElse(METADATA_IDENTIFIER, DEFAULT_STEP_NAME)

  override def getSubDataSourcesMetadata(implicit sparkSession: SparkSession): Array[Map[String, String]] = {
    val datasets = getDatasetsFromSource

    datasets.map(ds => {
      val baseOptions = Map(
        OPEN_LINEAGE_NAMESPACE -> NAMESPACE,
        METADATA_IDENTIFIER -> toMetadataIdentifier(ds)
      )
      if (format.equalsIgnoreCase(JDBC)) baseOptions ++ Map(JDBC_TABLE -> ds.physicalName) else baseOptions
    }).toArray
  }

  override def getAdditionalColumnMetadata(implicit sparkSession: SparkSession): Dataset[ColumnMetadata] = {
    val datasets = getDatasetsFromSource

    val columnMetadata = datasets
      .flatMap(ds => {
        val dataSourceReadOptions = connectionConfig ++ Map(
          DATASET_NAME -> ds.id.name,
          METADATA_IDENTIFIER -> toMetadataIdentifier(ds)
        )
        val facets = ds.facets
        ds.fields
          .map(field => {
            val dataType = field.`type`.getOrElse(DEFAULT_FIELD_TYPE).toLowerCase
            val parsedDataType = if (dataType == "varchar" || dataType == "text") "string" else dataType
            var metadata = Map(
              FIELD_DATA_TYPE -> parsedDataType,
              FIELD_DESCRIPTION -> field.description.getOrElse(""),
            )
            if (facets.contains(FACET_DATA_SOURCE)) {
              val dataSourceFacet = ds.facets(FACET_DATA_SOURCE).asInstanceOf[Map[String, String]]
              metadata = metadata ++ Map(DATA_SOURCE_NAME -> dataSourceFacet("name"), URI -> dataSourceFacet(URI))
            }
            ColumnMetadata(field.name, dataSourceReadOptions, metadata)
          })
      })
    sparkSession.createDataset(columnMetadata)
  }


  override def close(): Unit = {
    asyncHttpClient.close()
  }

  private def getDatasetsFromSource: List[OpenLineageDataset] = {
    val datasets = OPT_DATASET.map(ds => List(getDataset(NAMESPACE, ds)))
      .getOrElse(listDatasets(NAMESPACE).datasets)
    datasets
  }

  def toMetadataIdentifier(dataset: OpenLineageDataset) = s"${dataset.id.namespace}_${dataset.id.name}"

  def getDataset(namespace: String, dataset: String): OpenLineageDataset = {
    val baseUrl = connectionConfig(METADATA_SOURCE_URL)
    val response = getResponse(s"$baseUrl/api/v1/namespaces/$namespace/datasets/$dataset")
    val tryParseResponse = Try(
      ObjectMapperUtil.jsonObjectMapper.readValue(response.getResponseBody, classOf[OpenLineageDataset])
    )
    getResponse(tryParseResponse)
  }

  def listDatasets(namespace: String): ListDatasetResponse = {
    val baseUrl = connectionConfig(METADATA_SOURCE_URL)
    val response = getResponse(s"$baseUrl/api/v1/namespaces/$namespace/datasets")
    val tryParseResponse = Try(
      ObjectMapperUtil.jsonObjectMapper.readValue(response.getResponseBody, classOf[ListDatasetResponse])
    )
    getResponse(tryParseResponse)
  }

  private def getResponse(url: String): Response = {
    val tryRequest = Try(asyncHttpClient.prepareGet(url).execute().get())
    tryRequest match {
      case Failure(exception) =>
        throw new RuntimeException(s"Failed to call HTTP url, url=$url", exception)
      case Success(value) =>
        value
    }
  }

  private def getResponse[T](tryParse: Try[T]): T = {
    tryParse match {
      case Failure(exception) =>
        LOGGER.error("Failed to parse response from Marquez")
        throw new RuntimeException(exception)
      case Success(value) =>
        LOGGER.debug("Successfully parse response from Marquez to OpenLineage definition")
        value
    }
  }

  private def getDataSourceType(dataset: OpenLineageDataset): String = {
    val hasDataSourceInfo = dataset.facets.contains(FACET_DATA_SOURCE)
    if (hasDataSourceInfo) {
      val dataSourceFacet = dataset.facets.get(FACET_DATA_SOURCE).asInstanceOf[Map[String, String]]
      val uri = dataSourceFacet(URI)

    }
    dataset.`type` match {
      case "DB_TABLE" => ???
      case "STREAM" => ???
    }
  }
}
