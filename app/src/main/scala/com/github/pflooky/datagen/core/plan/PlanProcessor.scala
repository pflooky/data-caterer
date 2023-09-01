package com.github.pflooky.datagen.core.plan

import com.github.pflooky.datacaterer.api.PlanRun
import com.github.pflooky.datacaterer.api.model.Constants.PLAN_CLASS
import com.github.pflooky.datacaterer.api.model.DataCatererConfiguration
import com.github.pflooky.datagen.core.config.ConfigParser
import com.github.pflooky.datagen.core.generator.DataGeneratorProcessor
import com.github.pflooky.datagen.core.generator.metadata.datasource.DataSourceMetadataFactory
import com.github.pflooky.datagen.core.util.SparkProvider
import org.apache.spark.sql.SparkSession

object PlanProcessor {

  def determineAndExecutePlan(): Unit = {
    val optPlanClass = getPlanClass
    optPlanClass.map(Class.forName)
      .map(_.getDeclaredConstructor().newInstance().asInstanceOf[PlanRun])
      .map(executePlan)
      .getOrElse(executePlan)
  }

  private def executePlan(planRun: PlanRun): Unit = {
    val dataCatererConfiguration = planRun._configuration
    implicit val sparkSession: SparkSession = new SparkProvider(dataCatererConfiguration.sparkMaster, dataCatererConfiguration.sparkConfig).getSparkSession

    val dataGeneratorProcessor = new DataGeneratorProcessor(dataCatererConfiguration)
    dataGeneratorProcessor.generateData(planRun._plan, planRun._tasks)
  }

  private def executePlan: Unit = {
    val dataCatererConfiguration = ConfigParser.toDataCatererConfiguration
    executePlanWithConfig(dataCatererConfiguration)
  }

  private def executePlanWithConfig(dataCatererConfiguration: DataCatererConfiguration): Unit = {
    implicit val sparkSession: SparkSession = new SparkProvider(dataCatererConfiguration.sparkMaster, dataCatererConfiguration.sparkConfig).getSparkSession

    val optPlanWithTasks = new DataSourceMetadataFactory(dataCatererConfiguration).extractAllDataSourceMetadata()
    val dataGeneratorProcessor = new DataGeneratorProcessor(dataCatererConfiguration)
    optPlanWithTasks
      .map(x => dataGeneratorProcessor.generateData(x._1, x._2))
      .getOrElse(dataGeneratorProcessor.generateData())
  }

  private def getPlanClass: Option[String] = {
    val envPlanClass = System.getenv(PLAN_CLASS)
    val propPlanClass = System.getProperty(PLAN_CLASS)
    (envPlanClass, propPlanClass) match {
      case (env, _) if env != null && env.nonEmpty => Some(env)
      case (_, prop) if prop != null && prop.nonEmpty => Some(prop)
      case _ => None
    }
  }
}