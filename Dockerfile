FROM apache/spark:v3.3.2

USER root
RUN groupadd -g 1001 app && useradd -m -u 1001 -g app app
RUN mkdir -p /opt/app
RUN chown -R app:app /opt/app
COPY --chown=app:app app/build/libs/*.jar /opt/app/job.jar
COPY --chown=app:app script/run-spark.sh /opt/app/run-spark.sh
RUN chmod 755 -R /opt/app

USER app

ENTRYPOINT ["/opt/app/run-spark.sh"]
