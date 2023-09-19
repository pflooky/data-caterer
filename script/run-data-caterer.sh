#!/usr/bin/env bash
DATA_CATERER_MASTER="${DATA_CATERER_MASTER:-local[*]}"
DEPLOY_MODE="${DEPLOY_MODE:-client}"
JAVA_OPTS="-Dlog4j.configurationFile=file:///opt/app/log4j2.properties -Djdk.module.illegalAccess=deny"
DRIVER_MEMORY="${DRIVER_MEMORY:-1g}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-1g}"

/opt/spark/bin/spark-submit \
  --class com.github.pflooky.datagen.App \
  --master "$DATA_CATERER_MASTER" \
  --deploy-mode "$DEPLOY_MODE" \
  --conf "spark.driver.extraJavaOptions=${JAVA_OPTS}" \
  --conf "spark.executor.extraJavaOptions=${JAVA_OPTS}" \
  --driver-memory "$DRIVER_MEMORY" \
  --executor-memory "$EXECUTOR_MEMORY" \
  /opt/app/job.jar
