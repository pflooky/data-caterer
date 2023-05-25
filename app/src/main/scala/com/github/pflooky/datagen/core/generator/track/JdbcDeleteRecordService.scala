package com.github.pflooky.datagen.core.generator.track

import com.github.pflooky.datagen.core.exception.{InvalidDataSourceOptions, UnsupportedJdbcDeleteDataType}
import com.github.pflooky.datagen.core.model.Constants.{JDBC_TABLE, PASSWORD, URL, USERNAME}
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.DriverManager

class JdbcDeleteRecordService extends DeleteRecordService {

  override def deleteRecords(dataSourceName: String, trackedRecords: DataFrame, options: Map[String, String])(implicit sparkSession: SparkSession): Unit = {
    val table = options.getOrElse(JDBC_TABLE, throw new InvalidDataSourceOptions(dataSourceName, JDBC_TABLE))
    val whereClauseColumns = trackedRecords.columns.map(c => s"$c = ?").mkString(" AND ")

    trackedRecords.rdd.foreachPartition(partition => {
      val url = options.getOrElse(URL, throw new InvalidDataSourceOptions(dataSourceName, URL))
      val username = options.getOrElse(USERNAME, throw new InvalidDataSourceOptions(dataSourceName, USERNAME))
      val password = options.getOrElse(PASSWORD, throw new InvalidDataSourceOptions(dataSourceName, PASSWORD))
      val connection = DriverManager.getConnection(url, username, password)
      val preparedStatement = connection.prepareStatement(s"DELETE FROM $table WHERE $whereClauseColumns")

      partition.grouped(BATCH_SIZE).foreach(batch => {
        batch.foreach(r => {
          r.schema.fields.zipWithIndex.foreach(field => {
            val preparedIndex = field._2 + 1
            field._1.dataType match {
              case StringType => preparedStatement.setString(preparedIndex, r.getString(field._2))
              case ShortType => preparedStatement.setShort(preparedIndex, r.getShort(field._2))
              case IntegerType => preparedStatement.setInt(preparedIndex, r.getInt(field._2))
              case LongType => preparedStatement.setLong(preparedIndex, r.getLong(field._2))
              case DecimalType() => preparedStatement.setBigDecimal(preparedIndex, r.getDecimal(field._2))
              case DoubleType => preparedStatement.setDouble(preparedIndex, r.getDouble(field._2))
              case FloatType => preparedStatement.setFloat(preparedIndex, r.getFloat(field._2))
              case BooleanType => preparedStatement.setBoolean(preparedIndex, r.getBoolean(field._2))
              case ByteType => preparedStatement.setByte(preparedIndex, r.getByte(field._2))
              case TimestampType => preparedStatement.setTimestamp(preparedIndex, r.getTimestamp(field._2))
              case DateType => preparedStatement.setDate(preparedIndex, r.getDate(field._2))
              case x => throw new UnsupportedJdbcDeleteDataType(x, table)
            }
          })
          preparedStatement.addBatch()
        })
        preparedStatement.executeBatch()
      })
      connection.close()
    })
  }

}
