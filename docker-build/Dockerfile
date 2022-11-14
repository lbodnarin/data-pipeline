FROM apache/airflow:2.4.2
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jre-headless procps && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ADD --chown=airflow:root requirements.rst .
RUN pip install --no-cache-dir -r requirements.rst