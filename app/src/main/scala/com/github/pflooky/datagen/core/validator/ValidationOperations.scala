package com.github.pflooky.datagen.core.validator

import com.github.pflooky.datacaterer.api.model.Constants.{AGGREGATION_COUNT, FORMAT, PATH, VALIDATION_PREFIX_JOIN_EXPRESSION, VALIDATION_UNIQUE}
import com.github.pflooky.datacaterer.api.model.{ExpressionValidation, GroupByValidation, Step, UpstreamDataSourceValidation, Validation}
import com.github.pflooky.datagen.core.model.Constants.RECORD_TRACKING_VALIDATION_FORMAT
import com.github.pflooky.datagen.core.model.ValidationResult
import com.github.pflooky.datagen.core.util.MetadataUtil
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, expr}

abstract class ValidationOps(validation: Validation) {
  def validate(df: DataFrame, dfCount: Long): ValidationResult

  def getIsSuccessAndSampleErrors(notEqualDf: Dataset[Row], dfCount: Long): (Boolean, Option[DataFrame], Long) = {
    val numErrors = notEqualDf.count()
    val (isSuccess, sampleErrors) = (numErrors, validation.errorThreshold) match {
      case (c, Some(threshold)) if c > 0 =>
        if ((threshold >= 1 && c > threshold) || (threshold < 1 && c.toDouble / dfCount > threshold)) {
          (false, Some(notEqualDf))
        } else (true, None)
      case (c, None) if c > 0 => (false, Some(notEqualDf))
      case _ => (true, None)
    }
    (isSuccess, sampleErrors, numErrors)
  }
}

class ExpressionValidationOps(expressionValidation: ExpressionValidation) extends ValidationOps(expressionValidation) {
  override def validate(df: DataFrame, dfCount: Long): ValidationResult = {
    val notEqualDf = df.where(s"!(${expressionValidation.expr})")
    val (isSuccess, sampleErrors, numErrors) = getIsSuccessAndSampleErrors(notEqualDf, dfCount)
    ValidationResult(expressionValidation, isSuccess, numErrors, dfCount, sampleErrors)
  }
}

class GroupByValidationOps(groupByValidation: GroupByValidation) extends ValidationOps(groupByValidation) {
  override def validate(df: DataFrame, dfCount: Long): ValidationResult = {
    val groupByDf = df.groupBy(groupByValidation.groupByCols.map(col): _*)
    val (aggregateDf, validationCount) = if ((groupByValidation.aggCol == VALIDATION_UNIQUE || groupByValidation.aggCol.isEmpty) && groupByValidation.aggType == AGGREGATION_COUNT) {
      (groupByDf.count(), 1L)
    } else {
      val aggDf = groupByDf.agg(Map(
        groupByValidation.aggCol -> groupByValidation.aggType
      ))
      (aggDf, aggDf.count())
    }
    val notEqualDf = aggregateDf.where(s"!(${groupByValidation.expr})")
    val (isSuccess, sampleErrors, numErrors) = getIsSuccessAndSampleErrors(notEqualDf, validationCount)
    ValidationResult(groupByValidation, isSuccess, numErrors, validationCount, sampleErrors)
  }
}

class UpstreamDataSourceValidationOps(upstreamDataSourceValidation: UpstreamDataSourceValidation, recordTrackingForValidationFolderPath: String) extends ValidationOps(upstreamDataSourceValidation) {
  override def validate(df: DataFrame, dfCount: Long): ValidationResult = {
    val upstreamConnectionOptions = upstreamDataSourceValidation.upstreamDataSource.connectionConfigWithTaskBuilder.options ++
      upstreamDataSourceValidation.upstreamReadOptions
    val upstreamFormat = upstreamConnectionOptions(FORMAT)
    val upstreamName = upstreamDataSourceValidation.upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName
    val upstreamStep = upstreamDataSourceValidation.upstreamDataSource.step.map(s => s.step.copy(options = upstreamConnectionOptions ++ s.step.options))
      .getOrElse(Step(options = upstreamConnectionOptions))
    val joinCols = upstreamDataSourceValidation.joinCols
    val joinType = upstreamDataSourceValidation.joinType
    val upstreamDataSourcePath = MetadataUtil.getSubDataSourcePath(upstreamName, upstreamFormat, upstreamStep, recordTrackingForValidationFolderPath)

    val upstreamDf = df.sparkSession.read
      .format(RECORD_TRACKING_VALIDATION_FORMAT)
      .option(PATH, upstreamDataSourcePath)
      .load()
    val upstreamColsToRename = upstreamDf.columns.filter(c => !joinCols.contains(c))
      .map(c => c -> s"${upstreamName}_$c").toMap
    val renamedUpstreamDf = upstreamDf.withColumnsRenamed(upstreamColsToRename)

    val joinedDf = if (joinCols.size == 1 && joinCols.head.startsWith(VALIDATION_PREFIX_JOIN_EXPRESSION)) {
      df.join(renamedUpstreamDf, expr(joinCols.head.replaceFirst(VALIDATION_PREFIX_JOIN_EXPRESSION, "")), joinType)
    } else {
      df.join(renamedUpstreamDf, joinCols, joinType)
    }
    if (!joinedDf.storageLevel.useMemory) joinedDf.cache()
    val joinedCount = joinedDf.count()

    val baseValidationOp = upstreamDataSourceValidation.validationBuilder.validation match {
      case expr: ExpressionValidation => new ExpressionValidationOps(expr)
      case grp: GroupByValidation => new GroupByValidationOps(grp)
      case up: UpstreamDataSourceValidation => new UpstreamDataSourceValidationOps(up, recordTrackingForValidationFolderPath)
      case x => throw new RuntimeException(s"Unsupported validation type, validation=$x")
    }
    val result = baseValidationOp.validate(joinedDf, joinedCount)
    ValidationResult.fromValidationWithBaseResult(upstreamDataSourceValidation, result)
  }
}
