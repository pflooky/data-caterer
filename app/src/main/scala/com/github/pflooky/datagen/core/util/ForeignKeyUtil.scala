package com.github.pflooky.datagen.core.util

import com.github.pflooky.datagen.core.generator.plan.datasource.database.ForeignKeyRelationship
import com.github.pflooky.datagen.core.model.SinkOptions
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset}
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
        LOGGER.debug(s"Getting source dataframe, source=$sourceDfName")
        if (!generatedDataForeachTask.contains(sourceDfName)) {
          throw new RuntimeException(s"Cannot create target foreign key as one of the data sources not created. Please ensure there exists a data source with name (<task step type>.<task step name>): $sourceDfName")
        }
        val sourceDf = generatedDataForeachTask(sourceDfName)

        foreignKeyDetails._2.map(target => {
          val targetDfName = target.getDataFrameName
          LOGGER.debug(s"Getting target dataframe, source=$targetDfName")
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
    LOGGER.debug(s"Attempting to join source DF keys with target DF, source=$sourceColumn, target=$targetColumn")
    val joinDf = distinctSourceKeys.join(distinctTargetKeys, Seq("_join_foreign_key"))
      .drop("_join_foreign_key")
    val res = targetDf.join(joinDf, targetColumn)
      .withColumn(targetColumn, col(s"_src_$sourceColumn"))
      .drop(s"_src_$sourceColumn")
    LOGGER.debug(s"Applied source DF keys with target DF, source=$sourceColumn, target=$targetColumn")
    res
  }

  //TODO: Need some way to understand potential relationships between fields of different data sources (i.e. correlations, word2vec) https://spark.apache.org/docs/latest/ml-features

  /**
   * Can have logic like this:
   * 1. Using column metadata, find columns in other data sources that have similar metadata based on data profiling
   * 2. Assign a score to how similar two columns are across data sources
   * 3. Get those pairs that are greater than a threshold score
   * 4. Group all foreign keys together
   * 4.1. Unsure how to determine what is the primary source of the foreign key (the one that has the most references to it?)
   * @param dataSourceForeignKeys
   * @return
   */
  def getAllForeignKeyRelationships(dataSourceForeignKeys: List[Dataset[ForeignKeyRelationship]]): Map[String, List[String]] = {
    dataSourceForeignKeys.flatMap(_.collect())
      .groupBy(x => x.foreignKeyRelation)
      .map(x => (x._1.toString, x._2.map(_.foreignKey.toString)))
  }
}
