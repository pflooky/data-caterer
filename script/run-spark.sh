#!/usr/bin/env bash
SPARK_MASTER="local[*]"
DEPLOY_MODE="client"

/opt/spark/bin/spark-submit --class com.github.pflooky.datagen.App --master $SPARK_MASTER --deploy-mode $DEPLOY_MODE /opt/app/job.jar