package com.github.pflooky.datagen.core.util

import com.github.pflooky.datagen.core.generator.plan.datasource.database.ForeignKeyRelationship
import com.github.pflooky.datagen.core.model.{Plan, SinkOptions}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.{DataFrame, Dataset}

object ForeignKeyUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Apply same values from source data frame columns to target foreign key columns
   *
   * @param sinkOptions              where foreign key definitions are defined
   * @param generatedDataForeachTask map of <dataSourceName>.<stepName> => generated data as dataframe
   * @return map of <dataSourceName>.<stepName> => dataframe
   */
  def getDataFramesWithForeignKeys(plan: Plan, generatedDataForeachTask: Map[String, DataFrame]): Map[String, DataFrame] = {
    val enabledSources = plan.tasks.filter(_.enabled).map(_.dataSourceName)
    val sinkOptions = plan.sinkOptions.get
    val foreignKeyAppliedDfs = sinkOptions.foreignKeys
      .map(fk => sinkOptions.getForeignKeyRelations(fk._1))
      .filter(fkr => {
        val isMainForeignKeySourceEnabled = enabledSources.contains(fkr._1.dataSource)
        val subForeignKeySources = fkr._2.map(_.dataSource)
        val isSubForeignKeySourceEnabled = subForeignKeySources.forall(enabledSources.contains)
        val disabledSubSources = subForeignKeySources.filter(s => !enabledSources.contains(s))

        if (!isMainForeignKeySourceEnabled) {
          LOGGER.warn(s"Foreign key data source is not enabled. Data source needs to be enabled for foreign key relationship " +
            s"to exist from generated data, data-source-name=${fkr._1.dataSource}")
        }
        if (!isSubForeignKeySourceEnabled) {
          LOGGER.warn(s"Sub data sources within foreign key relationship are not enabled, disabled-task")
        }
        isMainForeignKeySourceEnabled && isSubForeignKeySourceEnabled
      })
      .flatMap(foreignKeyDetails => {
        val sourceDfName = foreignKeyDetails._1.getDataFrameName
        LOGGER.debug(s"Getting source dataframe, source=$sourceDfName")
        if (!generatedDataForeachTask.contains(sourceDfName)) {
          throw new RuntimeException(s"Cannot create target foreign key as one of the data sources not created. Please ensure there exists a data source with name (<plan dataSourceName>.<task step name>): $sourceDfName")
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
    sourceDf.cache()
    targetDf.cache()
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
   *
   * @param dataSourceForeignKeys Foreign key relationships for each data source
   * @return Map of data source columns to respective foreign key columns (which may be in other data sources)
   */
  def getAllForeignKeyRelationships(dataSourceForeignKeys: List[Dataset[ForeignKeyRelationship]]): Map[String, List[String]] = {
    dataSourceForeignKeys.flatMap(_.collect())
      .groupBy(_.key)
      .map(x => (x._1.toString, x._2.map(_.foreignKey.toString)))
  }

  def getDeleteOrder(foreignKeys: Map[String, List[String]]): List[String] = {
    //given map of foreign key relationships, need to order the foreign keys by leaf nodes first, parents after
    //could be nested foreign keys
    //e.g. key1 -> key2
    //key2 -> key3
    //resulting order of deleting should be key3, key2, key1
    var visited = Set[String]()

    def getForeignKeyOrder(currKey: String): List[String] = {
      if (!visited.contains(currKey)) {
        visited = visited ++ Set(currKey)

        if (foreignKeys.contains(currKey)) {
          val children = foreignKeys(currKey)
          val nested = children.flatMap(c => {
            if (!visited.contains(c)) {
              val nestedChildren = getForeignKeyOrder(c)
              visited = visited ++ Set(c)
              nestedChildren
            } else {
              List()
            }
          })
          nested ++ List(currKey)
        } else {
          List(currKey)
        }
      } else {
        List()
      }
    }
    foreignKeys.flatMap(x => getForeignKeyOrder(x._1)).toList

    //    foreignKeys.foreach(fk => {
    //      val parent = fk._1
    //      fk._2.map(child => {
    //        val nestedChildFks = foreignKeys.getOrElse(child, List())
    //
    //      })
    //    })
    //    List()
  }
}
