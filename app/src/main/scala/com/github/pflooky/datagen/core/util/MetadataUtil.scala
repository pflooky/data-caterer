package com.github.pflooky.datagen.core.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.pflooky.datagen.core.model.Constants.{COUNT, COUNT_DISTINCT, MAX, MAXIMUM_LENGTH, MAXIMUM_VALUE, MEAN, MIN, MINIMUM_LENGTH, MINIMUM_VALUE, STANDARD_DEVIATION, SUMMARY_COL}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, Metadata, MetadataBuilder, StringType, StructField, StructType}

object MetadataUtil {

  private val OBJECT_MAPPER = new ObjectMapper()
  OBJECT_MAPPER.registerModule(DefaultScalaModule)
  private val mapStringToAnyClass = Map[String, Any]()

  def toMap(metadata: Metadata): Map[String, Any] = {
    OBJECT_MAPPER.readValue(metadata.json, mapStringToAnyClass.getClass)
  }

  def getFieldMetadata(sourceData: DataFrame): Array[StructField] = {
    val schema = sourceData.schema
    val summaryStatisticsDf = sourceData.summary(COUNT, COUNT_DISTINCT, MEAN, STANDARD_DEVIATION, MIN, MAX)

    val fieldsWithMetadata = schema.fields.map(field => {
      val baseMetadata = new MetadataBuilder().withMetadata(field.metadata)
      if (summaryStatisticsDf.columns.contains(field.name)) {
        val fieldSummary = summaryStatisticsDf.select(SUMMARY_COL, field.name)
        val count = fieldSummary.filter(s"$SUMMARY_COL == '$COUNT'").first()
        val countDistinct = fieldSummary.filter(s"$SUMMARY_COL == '$COUNT_DISTINCT'").first()
        val max = fieldSummary.filter(s"$SUMMARY_COL == '$MAX'").first()
        val mean = fieldSummary.filter(s"$SUMMARY_COL == '$MEAN'").first()
        val min = fieldSummary.filter(s"$SUMMARY_COL == '$MIN'").first()
        val stddev = fieldSummary.filter(s"$SUMMARY_COL == '$STANDARD_DEVIATION'").first()

        baseMetadata.putString(COUNT, count.getString(1))
        baseMetadata.putString(COUNT_DISTINCT, countDistinct.getString(1))
        field.dataType match {
          case StringType =>
            baseMetadata.putLong(MAXIMUM_LENGTH, max.getString(1).length)
            baseMetadata.putLong(MINIMUM_LENGTH, min.getString(1).length)
          case DoubleType =>
            baseMetadata.putDouble(MAXIMUM_VALUE, max.getString(1).toDouble)
            baseMetadata.putDouble(MINIMUM_VALUE, min.getString(1).toDouble)
            baseMetadata.putDouble(MEAN, mean.getString(1).toDouble)
            baseMetadata.putDouble(STANDARD_DEVIATION, stddev.getString(1).toDouble)
          case IntegerType | LongType =>
            baseMetadata.putLong(MAXIMUM_VALUE, max.getString(1).toLong)
            baseMetadata.putLong(MINIMUM_VALUE, min.getString(1).toLong)
            baseMetadata.putDouble(MEAN, mean.getString(1).toDouble)
            baseMetadata.putDouble(STANDARD_DEVIATION, stddev.getString(1).toDouble)
        }
      }
      StructField(field.name, field.dataType, field.nullable, baseMetadata.build())
    })
    fieldsWithMetadata
  }
}
