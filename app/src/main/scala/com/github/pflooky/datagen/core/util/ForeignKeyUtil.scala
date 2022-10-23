package com.github.pflooky.datagen.core.util

import com.github.pflooky.datagen.core.model.SinkOptions
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}

object ForeignKeyUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Apply same values from source data frame columns to target foreign key columns
   *
   * @param sinkOptions where foreign key definitions are defined
   * @param generatedDataForeachTask map of <sinkName>.<stepName> => generated data as dataframe
   * @return map of <sinkName>.<stepName> => dataframe
   */
  def getDataFramesWithForeignKeys(sinkOptions: SinkOptions, generatedDataForeachTask: Map[String, DataFrame]): Map[String, DataFrame] = {
    val foreignKeyAppliedDfs = sinkOptions.foreignKeys
      .flatMap(fk => {
        val foreignKeyDetails = sinkOptions.getForeignKeyRelations(fk._1)
        val sourceDfName = foreignKeyDetails._1.getDataFrameName
        LOGGER.info(s"Getting source dataframe, source=$sourceDfName")
        val sourceDf = generatedDataForeachTask(sourceDfName)

        foreignKeyDetails._2.map(target => {
          val targetDfName = target.getDataFrameName
          LOGGER.info(s"Getting target dataframe, source=$targetDfName")
          val targetDf = generatedDataForeachTask(targetDfName)
          (targetDfName, applyForeignKeysToTargetDf(sourceDf, targetDf, foreignKeyDetails._1.column, target.column))
        })
      })
    generatedDataForeachTask ++ foreignKeyAppliedDfs
  }

  private def applyForeignKeysToTargetDf(sourceDf: DataFrame, targetDf: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    val distinctSourceKeys = sourceDf.select(sourceColumn).distinct()
      .withColumnRenamed(sourceColumn, s"_src_$sourceColumn")
      .withColumn("_join_foreign_key", monotonically_increasing_id())
    val distinctTargetKeys = targetDf.select(targetColumn).distinct()
      .withColumn("_join_foreign_key", monotonically_increasing_id())
    LOGGER.info(s"Attempting to join source DF keys with target DF, source=$sourceColumn, target=$targetColumn")
    val joinDf = distinctSourceKeys.join(distinctTargetKeys, Seq("_join_foreign_key"))
      .drop("_join_foreign_key")
    joinDf.show(false)
    val res = targetDf.join(joinDf, targetColumn)
      .withColumn(targetColumn, col(s"_src_$sourceColumn"))
      .drop(s"_src_$sourceColumn")
    LOGGER.info(s"Applied source DF keys with target DF, source=$sourceColumn, target=$targetColumn")
    res.show(2, false)
    res
  }

}
