#!/usr/bin/env bash
SPARK_MASTER="local[*]"
DEPLOY_MODE="client"
LOG4J_PATH="-Dlog4j.configurationFile=file:///opt/app/log4j2.properties"

/opt/spark/bin/spark-submit \
  --class com.github.pflooky.datagen.App \
  --master "$SPARK_MASTER" \
  --deploy-mode "$DEPLOY_MODE" \
  --conf "spark.driver.extraJavaOptions=${LOG4J_PATH}" \
  --conf "spark.executor.extraJavaOptions=${LOG4J_PATH}" \
  /opt/app/job.jar
