# Airflow image for Airflow_Banking: the official image + the Docker CLI.
# The Docker CLI lets tasks run `docker exec spark-client-dungafk spark-submit ...` through the host's
# Docker socket (mounted into the scheduler, where LocalExecutor runs the tasks).
FROM apache/airflow:3.3.2

USER root
# Static docker CLI binary only (no daemon); copied from the official docker CLI image.
COPY --from=docker:27-cli /usr/local/bin/docker /usr/local/bin/docker
USER airflow
