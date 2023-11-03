package com.github.pflooky.datagen.core.plan

import com.github.pflooky.datacaterer.api.PlanRun
import com.github.pflooky.datacaterer.api.model.Constants.PLAN_CLASS
import com.github.pflooky.datacaterer.api.model.DataCatererConfiguration
import com.github.pflooky.datagen.core.config.ConfigParser
import com.github.pflooky.datagen.core.generator.DataGeneratorProcessor
import com.github.pflooky.datagen.core.generator.metadata.datasource.DataSourceMetadataFactory
import com.github.pflooky.datagen.core.util.{SparkProvider, TrialUtil}
import org.apache.spark.sql.SparkSession

import scala.util.{Success, Try}

object PlanProcessor {

  def determineAndExecutePlan(optPlanRun: Option[PlanRun] = None): Unit = {
    if (isTrialValid) {
      val optPlanClass = getPlanClass
      optPlanClass.map(Class.forName)
        .map(cls => {
          cls.getDeclaredConstructor().newInstance()
          val tryScalaPlan = Try(cls.getDeclaredConstructor().newInstance().asInstanceOf[PlanRun])
          val tryJavaPlan = Try(cls.getDeclaredConstructor().newInstance().asInstanceOf[com.github.pflooky.datacaterer.java.api.PlanRun])
          (tryScalaPlan, tryJavaPlan) match {
            case (Success(value), _) => value
            case (_, Success(value)) => value.getPlan
            case _ => throw new RuntimeException(s"Failed to load class as either Java or Scala PlanRun, class=${optPlanClass.get}")
          }
        })
        .map(executePlan)
        .getOrElse(
          optPlanRun.map(executePlan)
            .getOrElse(executePlan)
        )
    } else {
      //do nothing and exit
    }
  }

  def determineAndExecutePlanJava(planRun: com.github.pflooky.datacaterer.java.api.PlanRun): Unit =
    determineAndExecutePlan(Some(planRun.getPlan))

  private def executePlan(planRun: PlanRun): Unit = {
    val dataCatererConfiguration = planRun._configuration
    implicit val sparkSession: SparkSession = new SparkProvider(dataCatererConfiguration.master, dataCatererConfiguration.runtimeConfig).getSparkSession

    executePlanWithConfig(dataCatererConfiguration, Some(planRun))
  }

  private def executePlan: Unit = {
    val dataCatererConfiguration = ConfigParser.toDataCatererConfiguration
    executePlanWithConfig(dataCatererConfiguration, None)
  }

  private def executePlanWithConfig(dataCatererConfiguration: DataCatererConfiguration, optPlan: Option[PlanRun]): Unit = {
    implicit val sparkSession: SparkSession = new SparkProvider(dataCatererConfiguration.master, dataCatererConfiguration.runtimeConfig).getSparkSession

    val optPlanWithTasks = new DataSourceMetadataFactory(dataCatererConfiguration).extractAllDataSourceMetadata(optPlan)
    val dataGeneratorProcessor = new DataGeneratorProcessor(dataCatererConfiguration)
    (optPlanWithTasks, optPlan) match {
      case (Some((genPlan, genTasks, genValidation)), _) => dataGeneratorProcessor.generateData(genPlan, genTasks, Some(List(genValidation)))
      case (_, Some(plan)) => dataGeneratorProcessor.generateData(plan._plan, plan._tasks, Some(plan._validations))
      case _ => dataGeneratorProcessor.generateData()
    }
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

  private def isTrialValid: Boolean = {
    if (ConfigParser.applicationType == "trial") {
      TrialUtil.checkApiKey()
    } else true
  }
}
