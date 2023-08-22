package com.github.pflooky.datagen.core.model


case class FlagsConfig(
                        enableCount: Boolean,
                        enableGenerateData: Boolean,
                        enableRecordTracking: Boolean,
                        enableDeleteGeneratedRecords: Boolean,
                        enableGeneratePlanAndTasks: Boolean = false,
                        enableFailOnError: Boolean = true,
                        enableUniqueCheck: Boolean = false,
                        enableSinkMetadata: Boolean = true,
                        enableSaveSinkMetadata: Boolean = true,
                      ) {
  def this() = this(true, true, true, true)
}

case class FoldersConfig(
                          planFilePath: String,
                          taskFolderPath: String,
                          generatedPlanAndTaskFolderPath: String = "/tmp",
                          generatedDataResultsFolderPath: String = "/tmp",
                          recordTrackingFolderPath: String = "/tmp"
                        ) {
  def this() = this("", "")
}

case class MetadataConfig(
                           numRecordsFromDataSource: Int,
                           numRecordsForAnalysis: Int,
                           oneOfDistinctCountVsCountThreshold: Double = 0.2,
                           oneOfMinCount: Int = 1000,
                           numSinkSamples: Int = 10,
                         ) {
  def this() = this(1000, 1000)
}

case class GenerationConfig(
                             numRecordsPerBatch: Long = 100000,
                             numRecordsPerStep: Option[Long] = None,
                           ) {
  def this() = this(100000, None)
}
