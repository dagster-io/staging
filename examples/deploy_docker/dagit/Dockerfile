FROM python:3.7-slim

# Install git
RUN apt-get update && apt-get install -yqq git

RUN git clone https://github.com/dagster-io/dagster.git dagster \
    && cd dagster \
    && git checkout origin/dockerrunlauncherlib

RUN pip install \
    -e dagster/python_modules/dagit \
    -e dagster/python_modules/libraries/dagster-postgres \
    -e dagster/python_modules/libraries/dagster-docker

RUN mkdir -p /opt/dagster/dagster_home

ENV DAGSTER_HOME=/opt/dagster/dagster_home/

# Copy dagster instance YAML to $DAGSTER_HOME
COPY dagster.yaml workspace.yaml $DAGSTER_HOME

EXPOSE 3000

WORKDIR /opt/dagster/dagster_home/

ENTRYPOINT ["dagit", "-h", "0.0.0.0", "-p", "3000"]