package com.github.pflooky.datagen.core.util

import com.github.pflooky.datagen.core.model.Constants.IS_UNIQUE
import com.github.pflooky.datagen.core.model.{Task, TaskSummary}
import org.apache.spark.sql.{DataFrame, SparkSession}

class UniqueFieldUtil(executableTasks: List[(TaskSummary, Task)])(implicit sparkSession: SparkSession) {

  var uniqueFieldsDf: Map[UniqueField, DataFrame] = getUniqueFields

  def getUniqueFieldsValues(dataSourceStep: String, df: DataFrame): DataFrame = {
    //get all the unique values that have been generated for each column so far
    val existingFieldValues = uniqueFieldsDf.filter(uniqueDf => uniqueDf._1.getDataSourceName == dataSourceStep)
    var finalDf = df

    //drop duplicate records for data via dropDuplicates and then anti join with previously generated values
    existingFieldValues.foreach(existingCol => {
      val column = existingCol._1.column
      val dfWithUnique = finalDf.dropDuplicates(column)

      if (!existingCol._2.isEmpty) {
        finalDf = dfWithUnique.join(existingCol._2, column, "left_anti")
      } else {
        finalDf = dfWithUnique
      }
    })

    //update the map with the latest values
    existingFieldValues.foreach(col => uniqueFieldsDf = uniqueFieldsDf ++ Map(col._1 -> finalDf.select(col._1.column)))
    finalDf
  }

  private def getUniqueFields: Map[UniqueField, DataFrame] = {
    val uniqueFields = executableTasks.flatMap(t => {
      t._2.steps
        .flatMap(step => {
          step.schema.fields.getOrElse(List())
            .flatMap(f => {
              val isUnique = f.generator.flatMap(gen => {
                gen.options.get(IS_UNIQUE).map(_.toString.toBoolean)
              }).getOrElse(false)
              if (isUnique) Some(UniqueField(t._1.dataSourceName, step.name, f.name)) else None
            })
        })
    })
    uniqueFields.map(uc => (uc, sparkSession.emptyDataFrame)).toMap
  }

}

case class UniqueField(dataSource: String, step: String, column: String) {
  def getDataSourceName: String = s"$dataSource.$step"
}
