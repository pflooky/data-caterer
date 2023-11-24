package com.github.pflooky.datagen.core.generator.track

import com.github.pflooky.datacaterer.api.model.Constants.{PARQUET, PATH}
import com.github.pflooky.datacaterer.api.model.Step
import com.github.pflooky.datagen.core.model.Constants.RECORD_TRACKING_VALIDATION_FORMAT
import com.github.pflooky.datagen.core.model.PlanImplicits.StepOps
import com.github.pflooky.datagen.core.util.MetadataUtil.getSubDataSourcePath
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}

class RecordTrackingProcessor(recordTrackingFolderPath: String) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def trackRecords(df: DataFrame, dataSourceName: String, format: String, step: Step): Unit = {
    val subDataSourcePath = getSubDataSourcePath(dataSourceName, format, step, recordTrackingFolderPath)
    LOGGER.info(s"Generated record tracking is enabled, data-source-name=$dataSourceName, format=$format, save-path=$subDataSourcePath")
    df.write.format(RECORD_TRACKING_VALIDATION_FORMAT)
      .mode(SaveMode.Append)
      .option(PATH, subDataSourcePath)
      .save()
  }
}
