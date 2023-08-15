#!/usr/bin/env bash
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
DEPLOY_MODE="${DEPLOY_MODE:-client}"
LOG4J_PATH="-Dlog4j.configurationFile=file:///opt/app/log4j2.properties"
DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-1g}"
EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-1g}"

/opt/spark/bin/spark-submit \
  --class com.github.pflooky.datagen.App \
  --master "$SPARK_MASTER" \
  --deploy-mode "$DEPLOY_MODE" \
  --conf "spark.driver.extraJavaOptions=${LOG4J_PATH}" \
  --conf "spark.executor.extraJavaOptions=${LOG4J_PATH}" \
  --driver-memory "$DRIVER_MEMORY" \
  --executor-memory "$EXECUTOR_MEMORY" \
  /opt/app/job.jar
