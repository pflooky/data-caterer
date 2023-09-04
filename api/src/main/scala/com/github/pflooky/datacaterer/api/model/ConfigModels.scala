package com.github.pflooky.datacaterer.api.model

case class FlagsConfig(
                        enableCount: Boolean = true,
                        enableGenerateData: Boolean = true,
                        enableRecordTracking: Boolean = false,
                        enableDeleteGeneratedRecords: Boolean = false,
                        enableGeneratePlanAndTasks: Boolean = false,
                        enableFailOnError: Boolean = true,
                        enableUniqueCheck: Boolean = false,
                        enableSinkMetadata: Boolean = false,
                        enableSaveReports: Boolean = true,
                        enableValidation: Boolean = false,
                      )

case class FoldersConfig(
                          planFilePath: String = "/opt/app/plan/customer-create-plan.yaml",
                          taskFolderPath: String = "/opt/app/task",
                          generatedPlanAndTaskFolderPath: String = "/tmp",
                          generatedReportsFolderPath: String = "/opt/app/report",
                          recordTrackingFolderPath: String = "/opt/app/record-tracking",
                          validationFolderPath: String = "/opt/app/validation",
                        )

case class MetadataConfig(
                           numRecordsFromDataSource: Int = 10000,
                           numRecordsForAnalysis: Int = 10000,
                           oneOfDistinctCountVsCountThreshold: Double = 0.2,
                           oneOfMinCount: Int = 1000,
                           numGeneratedSamples: Int = 10,
                         )

case class GenerationConfig(
                             numRecordsPerBatch: Long = 100000,
                             numRecordsPerStep: Option[Long] = None,
                           )

case class DataCatererConfiguration(
                                     flagsConfig: FlagsConfig = FlagsConfig(),
                                     foldersConfig: FoldersConfig = FoldersConfig(),
                                     metadataConfig: MetadataConfig = MetadataConfig(),
                                     generationConfig: GenerationConfig = GenerationConfig(),
                                     connectionConfigByName: Map[String, Map[String, String]] = Map(),
                                     sparkConfig: Map[String, String] = Map(
                                       "spark.sql.cbo.enabled" -> "true",
                                       "spark.sql.adaptive.enabled" -> "true",
                                       "spark.sql.cbo.planStats.enabled" -> "true",
                                       "spark.sql.legacy.allowUntypedScalaUDF" -> "true",
                                       "spark.sql.statistics.histogram.enabled" -> "true",
                                       "spark.sql.shuffle.partitions" -> "10",
                                       "spark.sql.catalog.postgres" -> "",
                                       "spark.sql.catalog.cassandra" -> "com.datastax.spark.connector.datasource.CassandraCatalog",
                                       "spark.hadoop.fs.s3a.directory.marker.retention" -> "keep",
                                       "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled" -> "true",
                                     ),
                                     sparkMaster: String = "local[*]"
                                   )
