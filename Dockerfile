FROM apache/spark:v3.3.0

USER root
RUN mkdir -p /opt/app
RUN chown -R app:app /opt/app
COPY --chown=app:app build/libs/*.jar /opt/app/job.jar
RUN chmod 755 -R /opt/app

USER app
