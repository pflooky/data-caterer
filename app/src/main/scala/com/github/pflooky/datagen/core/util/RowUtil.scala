package com.github.pflooky.datagen.core.util

import org.apache.log4j.Logger
import org.apache.spark.sql.Row

import scala.util.{Failure, Success, Try}

object RowUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def getRowValue[T](row: Row, colName: String, default: T = null): T = {
    val hasColumn = row.schema.fields.exists(_.name.equalsIgnoreCase(colName))
    if (hasColumn) {
      val tryGetAsType = Try(row.getAs[T](colName))
      tryGetAsType match {
        case Failure(exception) =>
          val message = s"Failed to get column as data type, column-name=$colName, exception=$exception"
          LOGGER.error(message)
          throw new RuntimeException(message, exception)
        case Success(value) => value
      }
    } else if (default == null) {
      throw new RuntimeException(s"Invalid schema definition due to missing column, column-name=$colName")
    } else {
      LOGGER.debug(s"Column missing from schema definition, will revert to default value, column-name=$colName, default=$default")
      default
    }
  }

}
