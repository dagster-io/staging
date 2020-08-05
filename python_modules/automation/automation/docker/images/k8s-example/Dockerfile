ARG BASE_IMAGE
FROM "${BASE_IMAGE}"

ARG DAGSTER_VERSION

RUN apt-get update -yqq

RUN pip install \
    dagster==${DAGSTER_VERSION} \

ADD example_project .
