/*
 * This Scala source file was generated by the Gradle 'init' task.
 */
package com.github.pflooky.datagen

import com.github.pflooky.datagen.core.generator.DataGeneratorProcessor
import com.github.pflooky.datagen.core.generator.plan.datasource.DataSourceMetadataFactory
import org.apache.log4j.Logger

import java.time.{Duration, LocalDateTime}

object App {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val startTime = LocalDateTime.now()
    val optPlanWithTasks = new DataSourceMetadataFactory().extractAllDataSourceMetadata()
    val dataGeneratorProcessor = new DataGeneratorProcessor()
    if (optPlanWithTasks.isDefined) {
      val (plan, tasks) = optPlanWithTasks.get
      LOGGER.info("Will generate data based off the metadata generated from data sources defined in application.conf")
      dataGeneratorProcessor.generateData(plan, tasks)
    } else {
      LOGGER.info("Will generate data based off pre-defined plan and task files")
      dataGeneratorProcessor.generateData()
    }
    val endTime = LocalDateTime.now()
    val duration = Duration.between(startTime, endTime)
    LOGGER.info(s"Completed in ${duration.toSeconds}s")
  }
}
