package com.github.pflooky.datagen.core.generator.track

import com.github.pflooky.datacaterer.api.model.Constants.{PARQUET, PATH}
import com.github.pflooky.datacaterer.api.model.Step
import com.github.pflooky.datagen.core.model.PlanImplicits.StepOps
import com.github.pflooky.datagen.core.util.MetadataUtil.getSubDataSourcePath
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}

class RecordTrackingProcessor(recordTrackingFolderPath: String) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def trackRecords(df: DataFrame, dataSourceName: String, format: String, step: Step): Unit = {
    LOGGER.info(s"Generated record tracking is enabled, data-source-name=$dataSourceName, format=$format")
    if (format.equalsIgnoreCase("jms") || format.equalsIgnoreCase("http")) {
      LOGGER.warn(s"Record tracking for current format of data is not supported as data format does not natively support deleting, data-source-name=$dataSourceName, format=$format")
    } else {
      val primaryKeys = step.gatherPrimaryKeys
      val columnsToTrack = if (primaryKeys.isEmpty) df.columns.toList else primaryKeys
      val subDataSourcePath = getSubDataSourcePath(dataSourceName, format, step.options, recordTrackingFolderPath)
      df.selectExpr(columnsToTrack: _*)
        .write.format(PARQUET)
        .mode(SaveMode.Append)
        .option(PATH, subDataSourcePath)
        .save()
    }
  }
}
