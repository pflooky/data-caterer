package com.github.pflooky.datagen.core.exception

import com.github.pflooky.datagen.core.model.{Field, Step}
import org.apache.spark.sql.types.{DataType, StructField}

class PlanFileNotFoundException(filePath: String) extends RuntimeException {
  override def getMessage: String = s"Plan file does not exist. Define in application.conf under plan-file-path or via env var PLAN_FILE_PATH, plan-file-path: $filePath"
}

class TaskFolderNotDirectoryException(folderPath: String) extends RuntimeException {
  override def getMessage: String = s"Task folder defined is not a directory. Define in application.conf under task-folder-path or via env var TASK_FOLDER_PATH, task-folder-path: $folderPath"
}
class TaskParseException(taskFileName: String, throwable: Throwable) extends RuntimeException(throwable) {
  override def getMessage: String = s"Failed to parse task from file, task-file-name: $taskFileName"
}

class ForeignKeyFormatException(foreignKey: String) extends RuntimeException {
  override def getMessage: String = s"Foreign key should be split by '.' according to format: <dataSourceName>.<stepName>.<columnName>, foreign-key=$foreignKey"
}

class UnsupportedDataGeneratorType(returnType: String) extends RuntimeException {
  override def getMessage: String = s"Unsupported return type for data generator: type=$returnType"
}

class UnsupportedRealTimeDataSourceFormat(format: String) extends RuntimeException {
  override def getMessage: String = s"Unsupported data source format for creating real-time data, format=$format"
}

class UnsupportedJdbcDeleteDataType(dataType: DataType, table: String) extends RuntimeException {
  override def getMessage: String = s"Unsupported data type for deleting from JDBC data source: type=$dataType, table=$table"
}

class InvalidDataGeneratorConfigurationException(structField: StructField, undefinedMetadataField: String) extends RuntimeException {
  override def getMessage: String = s"Undefined configuration in metadata for the data generator defined. Please help to define 'undefined-metadata-field' " +
    s"in field 'metadata' to allow data to be generated, " +
    s"name=${structField.name}, data-type=${structField.dataType}, undefined-metadata-field=$undefinedMetadataField, metadata=${structField.metadata}"
}

class InvalidCountGeneratorConfigurationException(step: Step) extends RuntimeException {
  override def getMessage: String = s"'total' or 'generator' needs to be defined in count for step, step-name=${step.name}, schema=${step.schema}, count=${step.count}"
}

class InvalidFieldConfigurationException(field: Field) extends RuntimeException {
  override def getMessage: String = s"Field should have ('generator' and 'type' defined) or 'schema' defined, name=${field.name}"
}

class InvalidDataSourceOptions(dataSourceName: String, missingConfig: String) extends RuntimeException {
  override def getMessage: String = s"Missing config for data source connection, data-source-name=$dataSourceName, missing-config=$missingConfig"
}
