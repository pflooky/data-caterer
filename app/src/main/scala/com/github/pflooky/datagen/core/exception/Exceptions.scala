package com.github.pflooky.datagen.core.exception

case class PlanFileNotFoundException(filePath: String) extends RuntimeException {
  override def getMessage: String = s"Plan file does not exist. Define in application.conf under plan-file-path or via env var PLAN_FILE_PATH, plan-file-path: $filePath"
}

case class TaskFolderNotDirectoryException(folderPath: String) extends RuntimeException {
  override def getMessage: String = s"Task folder defined is not a directory. Define in application.conf under task-folder-path or via env var TASK_FOLDER_PATH, task-folder-path: $folderPath"
}
case class TaskParseException(taskFileName: String, throwable: Throwable) extends RuntimeException(throwable) {
  override def getMessage: String = s"Failed to parse task from file, task-file-name: $taskFileName"
}

case class ForeignKeyFormatException(foreignKey: String) extends RuntimeException {
  override def getMessage: String = s"Foreign key should be split by '.' according to format: <sinkName>.<stepName>.<columnName>, foreign-key=$foreignKey"
}