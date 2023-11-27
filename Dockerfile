ARG SPARK_VERSION=3.4.1
FROM apache/spark:$SPARK_VERSION

USER root
RUN groupadd -g 1001 app && useradd -m -u 1001 -g app app
RUN mkdir -p /opt/app
RUN chown -R app:app /opt/app
COPY --chown=app:app script /opt/app
COPY --chown=app:app app/src/main/resources/application.conf /opt/app/application.conf
COPY --chown=app:app app/src/main/resources/log4j2.properties /opt/app/log4j2.properties
COPY --chown=app:app app/src/main/resources/report /opt/app/report

ARG APP_VERSION=0.1
COPY --chown=app:app app/build/libs/datacaterer-basic-${APP_VERSION}.jar /opt/app/job.jar
COPY --chown=app:app api/build/libs/datacaterer-api-${APP_VERSION}.jar /opt/spark/jars/datacaterer-api-${APP_VERSION}.jar
RUN chmod 755 -R /opt/app

RUN mkdir -p /opt/app/data-caterer/sample/json
RUN chown -R app:app /opt/app/data-caterer/sample/json

USER app
ENV APPLICATION_CONFIG_PATH=/opt/app/application.conf

ENTRYPOINT ["/opt/app/run-data-caterer.sh"]
