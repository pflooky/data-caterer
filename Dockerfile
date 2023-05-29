FROM apache/spark:v3.3.2

USER root
RUN groupadd -g 1001 app && useradd -m -u 1001 -g app app
RUN mkdir -p /opt/app
RUN chown -R app:app /opt/app
COPY --chown=app:app script /opt/app
COPY --chown=app:app app/build/libs/app-all.jar /opt/app/job.jar
RUN chmod 755 -R /opt/app

RUN mkdir -p /opt/app/spartagen/sample/json
RUN chown -R app:app /opt/app/spartagen/sample/json

USER app
ENV APPLICATION_CONFIG_PATH=/opt/app/application.conf

ENTRYPOINT ["/opt/app/run-spark.sh"]
