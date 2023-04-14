FROM apache/spark:v3.3.2

USER root
RUN mkdir -p /opt/app
RUN chown -R app:app /opt/app
COPY --chown=app:app app/build/libs/*.jar /opt/app/job.jar
RUN chmod 755 -R /opt/app

USER app
