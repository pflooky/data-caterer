package com.github.pflooky.datacaterer.api.model

import com.github.pflooky.datacaterer.api.model.Constants.{DEFAULT_ENABLE_COUNT, DEFAULT_ENABLE_DELETE_GENERATED_RECORDS, DEFAULT_ENABLE_FAIL_ON_ERROR, DEFAULT_ENABLE_GENERATE_DATA, DEFAULT_ENABLE_GENERATE_PLAN_AND_TASKS, DEFAULT_ENABLE_RECORD_TRACKING, DEFAULT_ENABLE_SAVE_REPORTS, DEFAULT_ENABLE_SINK_METADATA, DEFAULT_ENABLE_UNIQUE_CHECK, DEFAULT_ENABLE_VALIDATION, DEFAULT_GENERATED_PLAN_AND_TASK_FOLDER_PATH, DEFAULT_GENERATED_REPORTS_FOLDER_PATH, DEFAULT_NUM_GENERATED_SAMPLES, DEFAULT_NUM_RECORDS_PER_BATCH, DEFAULT_NUM_RECORD_FOR_ANALYSIS, DEFAULT_NUM_RECORD_FROM_DATA_SOURCE, DEFAULT_ONE_OF_DISTINCT_COUNT_VS_COUNT_THRESHOLD, DEFAULT_ONE_OF_MIN_COUNT, DEFAULT_PLAN_FILE_PATH, DEFAULT_RECORD_TRACKING_FOLDER_PATH, DEFAULT_SPARK_CONFIG, DEFAULT_SPARK_MASTER, DEFAULT_TASK_FOLDER_PATH, DEFAULT_VALIDATION_FOLDER_PATH}

import scala.collection.JavaConverters.mapAsScalaMapConverter

case class FlagsConfig(
                        enableCount: Boolean = DEFAULT_ENABLE_COUNT,
                        enableGenerateData: Boolean = DEFAULT_ENABLE_GENERATE_DATA,
                        enableRecordTracking: Boolean = DEFAULT_ENABLE_RECORD_TRACKING,
                        enableDeleteGeneratedRecords: Boolean = DEFAULT_ENABLE_DELETE_GENERATED_RECORDS,
                        enableGeneratePlanAndTasks: Boolean = DEFAULT_ENABLE_GENERATE_PLAN_AND_TASKS,
                        enableFailOnError: Boolean = DEFAULT_ENABLE_FAIL_ON_ERROR,
                        enableUniqueCheck: Boolean = DEFAULT_ENABLE_UNIQUE_CHECK,
                        enableSinkMetadata: Boolean = DEFAULT_ENABLE_SINK_METADATA,
                        enableSaveReports: Boolean = DEFAULT_ENABLE_SAVE_REPORTS,
                        enableValidation: Boolean = DEFAULT_ENABLE_VALIDATION,
                      )

case class FoldersConfig(
                          planFilePath: String = DEFAULT_PLAN_FILE_PATH,
                          taskFolderPath: String = DEFAULT_TASK_FOLDER_PATH,
                          generatedPlanAndTaskFolderPath: String = DEFAULT_GENERATED_PLAN_AND_TASK_FOLDER_PATH,
                          generatedReportsFolderPath: String = DEFAULT_GENERATED_REPORTS_FOLDER_PATH,
                          recordTrackingFolderPath: String = DEFAULT_RECORD_TRACKING_FOLDER_PATH,
                          validationFolderPath: String = DEFAULT_VALIDATION_FOLDER_PATH,
                        )

case class MetadataConfig(
                           numRecordsFromDataSource: Int = DEFAULT_NUM_RECORD_FROM_DATA_SOURCE,
                           numRecordsForAnalysis: Int = DEFAULT_NUM_RECORD_FOR_ANALYSIS,
                           oneOfDistinctCountVsCountThreshold: Double = DEFAULT_ONE_OF_DISTINCT_COUNT_VS_COUNT_THRESHOLD,
                           oneOfMinCount: Long = DEFAULT_ONE_OF_MIN_COUNT,
                           numGeneratedSamples: Int = DEFAULT_NUM_GENERATED_SAMPLES,
                         )

case class GenerationConfig(
                             numRecordsPerBatch: Long = DEFAULT_NUM_RECORDS_PER_BATCH,
                             numRecordsPerStep: Option[Long] = None,
                           )

case class DataCatererConfiguration(
                                     flagsConfig: FlagsConfig = FlagsConfig(),
                                     foldersConfig: FoldersConfig = FoldersConfig(),
                                     metadataConfig: MetadataConfig = MetadataConfig(),
                                     generationConfig: GenerationConfig = GenerationConfig(),
                                     connectionConfigByName: Map[String, Map[String, String]] = Map(),
                                     sparkConfig: Map[String, String] = DEFAULT_SPARK_CONFIG.asScala.toMap,
                                     sparkMaster: String = DEFAULT_SPARK_MASTER
                                   )
