####################################################################################################
#
# DAGSTER UNIT IMAGE
#
# We use this Dockerfile to build an unit test image for our Buildkite CI/CD pipeline.
#
####################################################################################################
ARG BASE_IMAGE
ARG PYTHON_VERSION
FROM "${BASE_IMAGE}" AS snapshot_builder

RUN apt-get update -yqq \
    && apt-get install -yqq \
        build-essential \
        cron \
        g++ \
        gcc \
        git \
        make

RUN git clone https://github.com/dagster-io/dagster.git dagster \
    && cd dagster \
    && git checkout 93dbcbc7ab8d280e5607e86217a3e77c93985373

RUN cd dagster/python_modules \
    && pip install \
    -e dagster \
    -e dagster-graphql \
    -e dagit \
    -e libraries/dagster-celery[flower,redis,kubernetes] \
    -e libraries/dagster-cron \
    -e libraries/dagster-postgres \
    -e libraries/dagster-pandas \
    -e libraries/dagster-aws \
    -e libraries/dagster-gcp \
    -e libraries/dagster-k8s \
    -e libraries/dagster-celery-k8s \
    && pip freeze --exclude-editable > /snapshot-reqs.txt


FROM "${BASE_IMAGE}"

COPY --from=snapshot_builder /snapshot-reqs.txt .

# gcc etc needed for building gevent
RUN apt-get update -yqq \
    && apt-get install -yqq \
        build-essential \
        cron \
        g++ \
        gcc \
        git \
        make

RUN pip install -r /snapshot-reqs.txt \
    && rm /snapshot-reqs.txt

RUN apt-get remove -yqq \
        build-essential \
        g++ \
        gcc \
        git \
        make \
    && apt-get autoremove -yqq --purge \
    && apt-get clean
