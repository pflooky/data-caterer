package com.github.pflooky.datagen.core.generator.metadata.datasource.http

import com.github.pflooky.datacaterer.api.model.Constants.{ARRAY_MAXIMUM_LENGTH, ARRAY_MINIMUM_LENGTH, DEFAULT_HTTP_HEADERS_DATA_TYPE, DEFAULT_HTTP_HEADERS_INNER_DATA_TYPE, ENABLED_NULL, FIELD_DATA_TYPE, HTTP_HEADER_PARAMETER, HTTP_PARAMETER_TYPE, HTTP_PATH_PARAMETER, HTTP_QUERY_PARAMETER, IS_NULLABLE, MAXIMUM, MAXIMUM_LENGTH, MINIMUM, MINIMUM_LENGTH, ONE_OF_GENERATOR, ONE_OF_GENERATOR_DELIMITER, POST_SQL_EXPRESSION, REGEX_GENERATOR, SQL_GENERATOR, STATIC}
import com.github.pflooky.datacaterer.api.model.{ArrayType, BinaryType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType, StringType, StructType, TimestampType}
import com.github.pflooky.datagen.core.generator.metadata.datasource.database.ColumnMetadata
import com.github.pflooky.datagen.core.model.Constants.{HTTP_HEADER_COL_PREFIX, HTTP_PATH_PARAM_COL_PREFIX, HTTP_QUERY_PARAM_COL_PREFIX, REAL_TIME_BODY_COL, REAL_TIME_BODY_CONTENT_COL, REAL_TIME_CONTENT_TYPE_COL, REAL_TIME_HEADERS_COL, REAL_TIME_METHOD_COL, REAL_TIME_URL_COL}
import io.swagger.v3.oas.models.PathItem.HttpMethod
import io.swagger.v3.oas.models.media.Schema
import io.swagger.v3.oas.models.parameters.Parameter
import io.swagger.v3.oas.models.parameters.Parameter.StyleEnum
import io.swagger.v3.oas.models.{OpenAPI, Operation}
import org.apache.log4j.Logger

import scala.annotation.tailrec
import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaSetConverter, mapAsScalaMapConverter}

class OpenAPIConverter(openAPI: OpenAPI = new OpenAPI()) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def toColumnMetadata(path: String, method: HttpMethod, operation: Operation, readOptions: Map[String, String]): List[ColumnMetadata] = {
    val requestBodyMetadata = getRequestBodyMetadata(operation)
    val params = if (operation.getParameters != null) operation.getParameters.asScala.toList else List()
    val headers = getHeaders(params)
    val pathParams = getPathParams(params)
    val queryParams = getQueryParams(params)
    val baseUrl = openAPI.getServers.get(0).getUrl
    val variableReplacedUrl = if (openAPI.getServers.get(0).getVariables != null) {
      val variables = openAPI.getServers.get(0).getVariables.asScala.map(v => (v._1, v._2.getDefault))
      variables.foldLeft(baseUrl)((url, v) => url.replace(s"{${v._1}}", v._2))
    } else baseUrl
    val urlCol = getUrl(variableReplacedUrl + path, pathParams, queryParams)
    val methodCol = getMethod(method)

    val allColumns = urlCol ++ methodCol ++ requestBodyMetadata ++ headers ++ pathParams ++ queryParams
    allColumns.map(c => c.copy(dataSourceReadOptions = readOptions))
  }

  def getRequestBodyMetadata(operation: Operation): List[ColumnMetadata] = {
    if (operation.getRequestBody != null) {
      val requestContent = operation.getRequestBody.getContent.asScala.head
      val requestContentType = requestContent._1
      val schema = requestContent._2.getSchema
      val columnsMetadata = getFieldMetadata(schema)
      val sqlGenerator = requestContentType.toLowerCase match {
        case "application/x-www-form-urlencoded" =>
          val colNameToValue = columnsMetadata.map(c => s"CONCAT('${c.column}=', CAST(`${c.column}` AS STRING))").mkString(",")
          s"ARRAY_JOIN(ARRAY($colNameToValue), '&')"
        case "application/json" =>
          s"TO_JSON($REAL_TIME_BODY_CONTENT_COL)"
        case x =>
          LOGGER.warn(s"Unsupported request body content type, defaulting to 'application/json', content-type=$x")
          s"TO_JSON($REAL_TIME_BODY_CONTENT_COL)"
      }
      val bodyContentColsDataType = columnsMetadata.map(_.metadata(FIELD_DATA_TYPE)).mkString(",")
      val bodyContentDataType = if (columnsMetadata.size > 1) s"struct<$bodyContentColsDataType>" else bodyContentColsDataType

      List(
        ColumnMetadata(REAL_TIME_BODY_COL, Map(), Map(FIELD_DATA_TYPE -> StringType.toString, SQL_GENERATOR -> sqlGenerator)),
        ColumnMetadata(REAL_TIME_BODY_CONTENT_COL, Map(), Map(FIELD_DATA_TYPE -> bodyContentDataType), columnsMetadata),
        ColumnMetadata(REAL_TIME_CONTENT_TYPE_COL, Map(), Map(STATIC -> requestContentType, FIELD_DATA_TYPE -> StringType.toString))
      ) ++ columnsMetadata
    } else List()
  }


  def getQueryParams(params: List[Parameter]): List[ColumnMetadata] = {
    val queryParams = params
      .filter(p => p.getIn != null && p.getIn == HTTP_QUERY_PARAMETER)
      .map(p => {
        val colName = s"$HTTP_QUERY_PARAM_COL_PREFIX${p.getName}"
        val sqlGenerator = p.getSchema.getType match {
          case "array" =>
            val style = if (p.getStyle == null) StyleEnum.FORM else p.getStyle
            val explode = if (p.getExplode == null) true else p.getExplode.booleanValue()
            val delimiter = (style, explode) match {
              case (StyleEnum.FORM, false) => ","
              case (StyleEnum.SPACEDELIMITED, false) => "%20"
              case (StyleEnum.PIPEDELIMITED, false) => "|"
              case _ => s"&${p.getName}="
            }
            s"""CASE WHEN ARRAY_SIZE($colName) > 0 THEN CONCAT('${p.getName}=', ARRAY_JOIN($colName, '$delimiter')) ELSE null END"""
          case _ => s"CONCAT('${p.getName}=', $colName)"
        }

        val metadata = Map(
          HTTP_PARAMETER_TYPE -> HTTP_QUERY_PARAMETER,
          POST_SQL_EXPRESSION -> sqlGenerator,
          IS_NULLABLE -> (!p.getRequired).toString,
          ENABLED_NULL -> (!p.getRequired).toString,
        ) ++ getSchemaMetadata(p.getSchema)
        ColumnMetadata(colName, Map(), metadata)
      })
    queryParams
  }

  def getPathParams(params: List[Parameter]): List[ColumnMetadata] = {
    params
      .filter(p => p.getIn != null && p.getIn == HTTP_PATH_PARAMETER)
      .map(p => {
        val metadata = Map(
          HTTP_PARAMETER_TYPE -> HTTP_PATH_PARAMETER,
          IS_NULLABLE -> (!p.getRequired).toString,
          ENABLED_NULL -> (!p.getRequired).toString,
        ) ++ getSchemaMetadata(p.getSchema)
        ColumnMetadata(s"$HTTP_PATH_PARAM_COL_PREFIX${p.getName}", Map(), metadata)
      })
  }

  def getHeaders(params: List[Parameter]): List[ColumnMetadata] = {
    val headerParams = params.filter(p => p.getIn != null && p.getIn == HTTP_HEADER_PARAMETER)
    if (headerParams.isEmpty) {
      List()
    } else {
      val headers = headerParams
        .map(p => {
          val headerValueMetadata = getSchemaMetadata(p.getSchema)
          val isNullableMap = if (p.getRequired) {
            Map(IS_NULLABLE -> "false")
          } else {
            Map(IS_NULLABLE -> "true", ENABLED_NULL -> "true")
          }
          val headerKey = ColumnMetadata("key", Map(), Map(STATIC -> p.getName))
          val headerVal = ColumnMetadata("value", Map(), headerValueMetadata)
          val cleanColName = p.getName.replaceAll("-", "_")
          ColumnMetadata(s"$HTTP_HEADER_COL_PREFIX$cleanColName", Map(), Map(FIELD_DATA_TYPE -> DEFAULT_HTTP_HEADERS_INNER_DATA_TYPE) ++ isNullableMap, List(headerKey, headerVal))
        })
      val headerSqlGenerator = s"ARRAY(${headers.map(c => s"${c.column}").mkString(",")})"
      val baseMetadata = Map(FIELD_DATA_TYPE -> DEFAULT_HTTP_HEADERS_DATA_TYPE, SQL_GENERATOR -> headerSqlGenerator)
      List(ColumnMetadata(REAL_TIME_HEADERS_COL, Map(), baseMetadata)) ++ headers
    }
  }

  def getMethod(method: HttpMethod): List[ColumnMetadata] = {
    List(ColumnMetadata(REAL_TIME_METHOD_COL, Map(), Map(STATIC -> method.name(), FIELD_DATA_TYPE -> StringType.toString)))
  }

  def getUrl(baseUrl: String, pathParams: List[ColumnMetadata], queryParams: List[ColumnMetadata]): List[ColumnMetadata] = {
    val sqlGenerator = urlSqlGenerator(baseUrl, pathParams, queryParams)
    List(ColumnMetadata(REAL_TIME_URL_COL, Map(), Map(SQL_GENERATOR -> sqlGenerator, FIELD_DATA_TYPE -> StringType.toString)))
  }

  def urlSqlGenerator(baseUrl: String, pathParams: List[ColumnMetadata], queryParams: List[ColumnMetadata]): String = {
    val urlWithPathParamReplace = pathParams.foldLeft(s"'$baseUrl'")((url, pathParam) => {
      val colName = pathParam.column
      val colNameWithoutPrefix = colName.replaceFirst(HTTP_PATH_PARAM_COL_PREFIX, "")
      val replaceValue = pathParam.metadata.getOrElse(POST_SQL_EXPRESSION, s"`$colName`")
      s"REPLACE($url, '{$colNameWithoutPrefix}', URL_ENCODE($replaceValue))"
    })
    val urlWithPathAndQuery = if (queryParams.nonEmpty) s"CONCAT($urlWithPathParamReplace, '?')" else urlWithPathParamReplace
    val combinedQueryParams = queryParams.map(q => s"CAST(${q.metadata.getOrElse(POST_SQL_EXPRESSION, s"`${q.column}``")} AS STRING)").mkString(",")
    val combinedQuerySql = s"URL_ENCODE(ARRAY_JOIN(ARRAY($combinedQueryParams), '&'))"
    s"CONCAT($urlWithPathAndQuery, $combinedQuerySql)"
  }

  def getFieldMetadata(schema: Schema[_]): List[ColumnMetadata] = {
    if (schema.getType == "array") {
      val arrayMetadata = getSchemaMetadata(schema)
      val arrayName = if (schema.getName != null) schema.getName else "array"
      List(ColumnMetadata(arrayName, Map(), arrayMetadata, getFieldMetadata(schema.getItems)))
    } else if (schema.getType == "object") {
      val objectMetadata = getSchemaMetadata(schema)
      val objectName = if (schema.getName != null) schema.getName else "object"
      val nestedCols = schema.getProperties.asScala.flatMap(prop => {
        prop._2.setName(prop._1)
        getFieldMetadata(prop._2)
      }).toList
      List(ColumnMetadata(objectName, Map(), objectMetadata, nestedCols))
    } else {
      val nestedFields = getFields(schema)
      if (nestedFields.nonEmpty) {
        nestedFields
          .map(field => {
            val fieldMetadata = getSchemaMetadata(field._2, Some(field._1), Some(schema))
            val nestedCols = if (field._2.getType == "array" && field._2.getItems != null) {
              getFieldMetadata(field._2.getItems)
            } else {
              getFieldMetadata(field._2)
            }
            ColumnMetadata(field._1, Map(), fieldMetadata, nestedCols)
          }).toList
      } else {
        val fieldMetadata = getSchemaMetadata(schema)
        List(ColumnMetadata(schema.getName, Map(), fieldMetadata))
      }
    }
  }

  @tailrec
  private def getFields(schema: Schema[_]): Set[(String, Schema[_])] = {
    if (schema.getProperties != null) {
      schema.getProperties.entrySet().asScala.map(entry => (entry.getKey, entry.getValue)).toSet
    } else if (schema.get$ref() != null) {
      val componentName = schema.get$ref().split("/").last
      getFields(openAPI.getComponents.getSchemas.get(componentName))
    } else if (schema.getType == "array") {
      val arrayName = if (schema.getName != null) schema.getName else "array"
      Set(arrayName -> schema)
    } else {
      Set()
    }
  }

  private def getSchemaMetadata(field: Schema[_], optFieldName: Option[String] = None, optParentSchema: Option[Schema[_]] = None): Map[String, String] = {
    val dataType = getDataType(field).toString
    val minMaxMap = getMinMax(field)
    val miscMap = getMiscMetadata(field, optFieldName, optParentSchema)
    Map(FIELD_DATA_TYPE -> dataType) ++ minMaxMap ++ miscMap
  }

  private def getDataType(schema: Schema[_]): DataType = {
    if (schema == null) {
      LOGGER.warn("Schema is missing from OpenAPI document, defaulting to data type string")
      StringType
    } else {
      (schema.getType, schema.getFormat) match {
        case ("string", "date") => DateType
        case ("string", "date-time") => TimestampType
        case ("string", "binary") => BinaryType
        case ("string", _) => StringType
        case ("number", "float") => FloatType
        case ("number", _) => DoubleType
        case ("integer", "int64") => LongType
        case ("integer", _) => IntegerType
        case ("array", _) =>
          val innerType = getDataType(schema.getItems)
          new ArrayType(innerType)
        case ("object" | null, _) =>
          val innerType = schema.getProperties.asScala.map(s => (s._1, getDataType(s._2))).toList
          new StructType(innerType)
        case (x, _) => DataType.fromString(x)
      }
    }
  }

  private def getMiscMetadata(field: Schema[_], optFieldName: Option[String], optParentSchema: Option[Schema[_]]): Map[String, String] = {
    val nullabilityMap = optParentSchema.map(schema => {
      val requiredFields = if (schema.getRequired != null) schema.getRequired.asScala.toList else List()
      if (requiredFields.contains(optFieldName.getOrElse(""))) {
        Map(IS_NULLABLE -> false)
      } else {
        Map(
          IS_NULLABLE -> true,
          ENABLED_NULL -> true
        )
      }
    }).getOrElse(Map())
    val oneOfMap = if (field.getEnum != null) {
      Map(ONE_OF_GENERATOR -> field.getEnum.asScala.map(_.toString).mkString(ONE_OF_GENERATOR_DELIMITER))
    } else Map()

    (nullabilityMap ++ oneOfMap ++ Map(REGEX_GENERATOR -> field.getPattern))
      .flatMap(getMetadataFromSchemaAsMap)
  }

  private def getMinMax(schema: Schema[_]): Map[String, String] = {
    val minMap = Map(
      MINIMUM -> schema.getMinimum,
      MINIMUM_LENGTH -> schema.getMinLength,
      ARRAY_MINIMUM_LENGTH -> schema.getMinItems
    ).flatMap(getMetadataFromSchemaAsMap)

    val maxMap = Map(
      MAXIMUM -> schema.getMaximum,
      MAXIMUM_LENGTH -> schema.getMaxLength,
      ARRAY_MAXIMUM_LENGTH -> schema.getMaxItems
    ).flatMap(getMetadataFromSchemaAsMap)

    minMap ++ maxMap
  }

  private def getMetadataFromSchemaAsMap[T](keyVal: (String, T)): Map[String, String] = {
    if (keyVal._2 != null) {
      Map(keyVal._1 -> keyVal._2.toString)
    } else {
      Map()
    }
  }
}
