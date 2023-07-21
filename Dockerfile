ARG SPARK_VERSION=3.4.1
FROM apache/spark:$SPARK_VERSION

USER root
RUN groupadd -g 1001 app && useradd -m -u 1001 -g app app
RUN mkdir -p /opt/app
RUN chown -R app:app /opt/app
COPY --chown=app:app script /opt/app
ARG APP_VERSION=0.1
COPY --chown=app:app app/build/libs/datacaterer-basic-${APP_VERSION}.jar /opt/app/job.jar
RUN chmod 755 -R /opt/app

RUN mkdir -p /opt/app/data-caterer/sample/json
RUN chown -R app:app /opt/app/data-caterer/sample/json

USER app
ENV APPLICATION_CONFIG_PATH=/opt/app/application.conf

ENTRYPOINT ["/opt/app/run-spark.sh"]
