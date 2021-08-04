FROM python:3.8.7-slim

# Install Java 11 (for pyspark 3) and confirm that it works
# Deal with slim variants not having man page directories (which causes "update-alternatives" to fail)
RUN mkdir -p /usr/share/man/man1 /usr/share/man/man2 && \
    apt-get update -yqq \
    && apt-get upgrade -yqq && \
    apt-get install -yqq openjdk-11-jdk-headless \
    openjdk-11-jre-headless && \
    java -version

# Install gcs-connector-hadoop2 to enable pyspark to write to GCP buckets
RUN mkdir -p /usr/local/share/pyspark \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install wget -yqq \
    && cd /usr/local/share/pyspark \
    && wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-2.0.1.jar

COPY ./* /

RUN pip install -e .
