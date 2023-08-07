package com.github.pflooky.datagen.core.util

import com.github.pflooky.datagen.core.generator.metadata.datasource.database.ForeignKeyRelationship
import com.github.pflooky.datagen.core.model.{ForeignKeyRelation, Plan}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.immutable.Queue
import scala.collection.mutable

object ForeignKeyUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Apply same values from source data frame columns to target foreign key columns
   *
   * @param plan                     where foreign key definitions are defined
   * @param generatedDataForeachTask map of <dataSourceName>.<stepName> => generated data as dataframe
   * @return map of <dataSourceName>.<stepName> => dataframe
   */
  def getDataFramesWithForeignKeys(plan: Plan, generatedDataForeachTask: Map[String, DataFrame]): List[(String, DataFrame)] = {
    val enabledSources = plan.tasks.filter(_.enabled).map(_.dataSourceName)
    val sinkOptions = plan.sinkOptions.get
    val foreignKeyRelations = sinkOptions.foreignKeys
      .map(fk => sinkOptions.gatherForeignKeyRelations(fk._1))
    val enabledForeignKeys = foreignKeyRelations
      .filter(fkr => {
        val isMainForeignKeySourceEnabled = enabledSources.contains(fkr._1.dataSource)
        val subForeignKeySources = fkr._2.map(_.dataSource)
        val isSubForeignKeySourceEnabled = subForeignKeySources.forall(enabledSources.contains)
        val disabledSubSources = subForeignKeySources.filter(s => !enabledSources.contains(s))
        val columnExistsMain = generatedDataForeachTask(fkr._1.dataFrameName).columns.contains(fkr._1.column)

        if (!isMainForeignKeySourceEnabled) {
          LOGGER.warn(s"Foreign key data source is not enabled. Data source needs to be enabled for foreign key relationship " +
            s"to exist from generated data, data-source-name=${fkr._1.dataSource}")
        }
        if (!isSubForeignKeySourceEnabled) {
          LOGGER.warn(s"Sub data sources within foreign key relationship are not enabled, disabled-task")
        }
        if (!columnExistsMain) {
          LOGGER.warn(s"Main column for foreign key references is not created, data-source-name=${fkr._1.dataSource}, column=${fkr._1.column}")
        }
        isMainForeignKeySourceEnabled && isSubForeignKeySourceEnabled && columnExistsMain
      })

    val foreignKeyAppliedDfs = enabledForeignKeys.flatMap(foreignKeyDetails => {
      val sourceDfName = foreignKeyDetails._1.dataFrameName
      LOGGER.debug(s"Getting source dataframe, source=$sourceDfName")
      if (!generatedDataForeachTask.contains(sourceDfName)) {
        throw new RuntimeException(s"Cannot create target foreign key as one of the data sources not created. Please ensure there exists a data source with name (<plan dataSourceName>.<task step name>): $sourceDfName")
      }
      val sourceDf = generatedDataForeachTask(sourceDfName)

      val sourceDfsWithForeignKey = foreignKeyDetails._2.map(target => {
        val targetDfName = target.dataFrameName
        LOGGER.debug(s"Getting target dataframe, source=$targetDfName")
        val targetDf = generatedDataForeachTask(targetDfName)
        if (targetDf.columns.contains(target.column)) {
          (targetDfName, applyForeignKeysToTargetDf(sourceDf, targetDf, foreignKeyDetails._1.column, target.column))
        } else {
          LOGGER.warn("Foreign key data source does not contain foreign key defined in plan, defaulting to base generated data")
          (targetDfName, targetDf)
        }
      })
      sourceDfsWithForeignKey
    })

    val insertOrder = getInsertOrder(foreignKeyRelations.map(f => (f._1.dataFrameName, f._2.map(_.dataFrameName))))
    val insertOrderDfs = insertOrder
      .filter(foreignKeyAppliedDfs.contains)
      .map(s => (s, foreignKeyAppliedDfs(s)))
    generatedDataForeachTask.toList.filter(t => !insertOrderDfs.exists(_._1.equalsIgnoreCase(t._1))) ++ insertOrderDfs
  }

  private def applyForeignKeysToTargetDf(sourceDf: DataFrame, targetDf: DataFrame, sourceColumn: String, targetColumn: String): DataFrame = {
    if (!sourceDf.storageLevel.useMemory) sourceDf.cache()
    if (!targetDf.storageLevel.useMemory) targetDf.cache()
    val distinctSourceKeys = zipWithIndex(
      sourceDf.select(sourceColumn).distinct()
        .withColumnRenamed(sourceColumn, s"_src_$sourceColumn")
    )
    val distinctTargetKeys = zipWithIndex(targetDf.select(targetColumn).distinct())
    LOGGER.debug(s"Attempting to join source DF keys with target DF, source=$sourceColumn, target=$targetColumn")
    val joinDf = distinctSourceKeys.join(distinctTargetKeys, Seq("_join_foreign_key"))
      .drop("_join_foreign_key")
    val res = targetDf.join(joinDf, targetColumn)
      .withColumn(targetColumn, col(s"_src_$sourceColumn"))
      .drop(s"_src_$sourceColumn")
    LOGGER.debug(s"Applied source DF keys with target DF, source=$sourceColumn, target=$targetColumn")
    res.cache()
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

  //get delete order
  def getInsertOrder(foreignKeys: Map[String, List[String]]): List[String] = {
    val result = mutable.ListBuffer.empty[String]
    val visited = mutable.Set.empty[String]

    def visit(table: String): Unit = {
      if (!visited.contains(table)) {
        visited.add(table)
        foreignKeys.getOrElse(table, List.empty).foreach(visit)
        result.prepend(table)
      }
    }

    foreignKeys.keys.foreach(visit)
    result.toList
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
  }

  private def zipWithIndex(df: DataFrame): DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(ln =>
        Row.fromSeq(ln._1.toSeq ++ Seq(ln._2))
      ),
      StructType(
        df.schema.fields ++ Array(StructField("_join_foreign_key", LongType, false))
      )
    )
  }
}
