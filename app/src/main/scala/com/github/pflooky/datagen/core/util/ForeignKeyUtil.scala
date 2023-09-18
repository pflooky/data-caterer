package com.github.pflooky.datagen.core.util

import com.github.pflooky.datacaterer.api.model.Constants.OMIT
import com.github.pflooky.datacaterer.api.model.{ForeignKeyRelation, Plan}
import com.github.pflooky.datagen.core.generator.metadata.datasource.database.ForeignKeyRelationship
import com.github.pflooky.datagen.core.model.PlanImplicits.{ForeignKeyRelationOps, SinkOptionsOps}
import com.github.pflooky.datagen.core.util.GeneratorUtil.applySqlExpressions
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

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
      .filter(fkr => isValidForeignKeyRelation(generatedDataForeachTask, enabledSources, fkr))
    var taskDfs = generatedDataForeachTask

    val foreignKeyAppliedDfs = enabledForeignKeys.flatMap(foreignKeyDetails => {
      val sourceDfName = foreignKeyDetails._1.dataFrameName
      LOGGER.debug(s"Getting source dataframe, source=$sourceDfName")
      if (!taskDfs.contains(sourceDfName)) {
        throw new RuntimeException(s"Cannot create target foreign key as one of the data sources not created. " +
          s"Please ensure there exists a data source with name (<plan dataSourceName>.<task step name>): $sourceDfName")
      }
      val sourceDf = taskDfs(sourceDfName)

      val sourceDfsWithForeignKey = foreignKeyDetails._2.map(target => {
        val targetDfName = target.dataFrameName
        LOGGER.debug(s"Getting target dataframe, source=$targetDfName")
        val targetDf = taskDfs(targetDfName)
        if (target.columns.forall(targetDf.columns.contains)) {
          (targetDfName, applyForeignKeysToTargetDf(sourceDf, targetDf, foreignKeyDetails._1.columns, target.columns))
        } else {
          LOGGER.warn("Foreign key data source does not contain foreign key defined in plan, defaulting to base generated data")
          (targetDfName, targetDf)
        }
      })
      taskDfs ++= sourceDfsWithForeignKey.toMap
      sourceDfsWithForeignKey
    })

    val insertOrder = getInsertOrder(foreignKeyRelations.map(f => (f._1.dataFrameName, f._2.map(_.dataFrameName))))
    val insertOrderDfs = insertOrder
      .filter(i => foreignKeyAppliedDfs.exists(f => f._1.equalsIgnoreCase(i)))
      .map(s => (s, foreignKeyAppliedDfs.filter(f => f._1.equalsIgnoreCase(s)).head._2))
    taskDfs.toList.filter(t => !insertOrderDfs.exists(_._1.equalsIgnoreCase(t._1))) ++ insertOrderDfs
  }

  private def isValidForeignKeyRelation(generatedDataForeachTask: Map[String, DataFrame], enabledSources: List[String], fkr: (ForeignKeyRelation, List[ForeignKeyRelation])) = {
    val isMainForeignKeySourceEnabled = enabledSources.contains(fkr._1.dataSource)
    val subForeignKeySources = fkr._2.map(_.dataSource)
    val isSubForeignKeySourceEnabled = subForeignKeySources.forall(enabledSources.contains)
    val disabledSubSources = subForeignKeySources.filter(s => !enabledSources.contains(s))
    val columnExistsMain = fkr._1.columns.forall(generatedDataForeachTask(fkr._1.dataFrameName).columns.contains)

    if (!isMainForeignKeySourceEnabled) {
      LOGGER.warn(s"Foreign key data source is not enabled. Data source needs to be enabled for foreign key relationship " +
        s"to exist from generated data, data-source-name=${fkr._1.dataSource}")
    }
    if (!isSubForeignKeySourceEnabled) {
      LOGGER.warn(s"Sub data sources within foreign key relationship are not enabled, disabled-task=${disabledSubSources.mkString(",")}")
    }
    if (!columnExistsMain) {
      LOGGER.warn(s"Main column for foreign key references is not created, data-source-name=${fkr._1.dataSource}, column=${fkr._1.columns}")
    }
    isMainForeignKeySourceEnabled && isSubForeignKeySourceEnabled && columnExistsMain
  }

  private def applyForeignKeysToTargetDf(sourceDf: DataFrame, targetDf: DataFrame, sourceColumns: List[String], targetColumns: List[String]): DataFrame = {
    if (!sourceDf.storageLevel.useMemory) sourceDf.cache()
    if (!targetDf.storageLevel.useMemory) targetDf.cache()
    val sourceColRename = sourceColumns.map(c => (c, s"_src_$c")).toMap
    val distinctSourceKeys = zipWithIndex(
      sourceDf.selectExpr(sourceColumns: _*).distinct()
        .withColumnsRenamed(sourceColRename)
    )
    val distinctTargetKeys = zipWithIndex(targetDf.selectExpr(targetColumns: _*).distinct())

    LOGGER.debug(s"Attempting to join source DF keys with target DF, source=${sourceColumns.mkString(",")}, target=${targetColumns.mkString(",")}")
    val joinDf = distinctSourceKeys.join(distinctTargetKeys, Seq("_join_foreign_key"))
      .drop("_join_foreign_key")
    val targetColRename = targetColumns.zip(sourceColumns).map(c => (c._1, col(s"_src_${c._2}"))).toMap
    val res = targetDf.join(joinDf, targetColumns)
      .withColumns(targetColRename)
      .drop(sourceColRename.values.toList: _*)

    LOGGER.debug(s"Applied source DF keys with target DF, source=${sourceColumns.mkString(",")}, target=${targetColumns.mkString(",")}")
    if (!res.storageLevel.useMemory) res.cache()
    //need to add back original metadata as it will use the metadata from the sourceDf and override the targetDf metadata
    val dfMetadata = combineMetadata(sourceDf, sourceColumns, targetDf, targetColumns, res)
    applySqlExpressions(dfMetadata, targetColumns, false)
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
  def getAllForeignKeyRelationships(dataSourceForeignKeys: List[Dataset[ForeignKeyRelationship]]): List[(String, List[String])] = {
    dataSourceForeignKeys.flatMap(_.collect())
      .groupBy(_.key)
      .map(x => (x._1.toString, x._2.map(_.foreignKey.toString)))
      .toList
  }

  //get delete order
  def getInsertOrder(foreignKeys: List[(String, List[String])]): List[String] = {
    val result = mutable.ListBuffer.empty[String]
    val visited = mutable.Set.empty[String]

    def visit(table: String): Unit = {
      if (!visited.contains(table)) {
        visited.add(table)
        foreignKeys.find(f => f._1 == table).map(_._2).getOrElse(List.empty).foreach(visit)
        result.prepend(table)
      }
    }

    foreignKeys.map(_._1).foreach(visit)
    result.toList
  }

  def getDeleteOrder(foreignKeys: List[(String, List[String])]): List[String] = {
    //given map of foreign key relationships, need to order the foreign keys by leaf nodes first, parents after
    //could be nested foreign keys
    //e.g. key1 -> key2
    //key2 -> key3
    //resulting order of deleting should be key3, key2, key1
    val fkMap = foreignKeys.toMap
    var visited = Set[String]()

    def getForeignKeyOrder(currKey: String): List[String] = {
      if (!visited.contains(currKey)) {
        visited = visited ++ Set(currKey)

        if (fkMap.contains(currKey)) {
          val children = foreignKeys.find(f => f._1 == currKey).map(_._2).getOrElse(List())
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

    foreignKeys.flatMap(x => getForeignKeyOrder(x._1))
  }

  private def zipWithIndex(df: DataFrame): DataFrame = {
    if (!df.storageLevel.useMemory) df.cache()
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(ln =>
        Row.fromSeq(ln._1.toSeq ++ Seq(ln._2))
      ),
      StructType(
        df.schema.fields ++ Array(StructField("_join_foreign_key", LongType, false))
      )
    )
  }

  private def combineMetadata(sourceDf: DataFrame, sourceCols: List[String], targetDf: DataFrame, targetCols: List[String], df: DataFrame): DataFrame = {
    val sourceColsMetadata = sourceCols.map(c => {
      val baseMetadata = sourceDf.schema.fields.find(_.name.equalsIgnoreCase(c)).get.metadata
      new MetadataBuilder().withMetadata(baseMetadata).remove(OMIT).build()
    })
    val targetColsMetadata = targetCols.map(c => (c, targetDf.schema.fields.find(_.name.equalsIgnoreCase(c)).get.metadata))
    val newMetadata = sourceColsMetadata.zip(targetColsMetadata).map(meta => (meta._2._1, new MetadataBuilder().withMetadata(meta._2._2).withMetadata(meta._1).build()))
    //also should apply any further sql statements
    newMetadata.foldLeft(df)((metaDf, meta) => metaDf.withMetadata(meta._1, meta._2))
  }
}
