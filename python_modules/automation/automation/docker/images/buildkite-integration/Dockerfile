####################################################################################################
#
# DAGSTER INTEGRATION IMAGE
#
# We use this Dockerfile to build an image for our Buildkite CI/CD pipeline.
#
####################################################################################################

ARG BASE_IMAGE
ARG PYTHON_VERSION
FROM "${BASE_IMAGE}" AS snapshot_builder

RUN git clone https://github.com/dagster-io/dagster.git dagster \
    && cd dagster \
    && git checkout 93dbcbc7ab8d280e5607e86217a3e77c93985373

RUN apt-get update \
    && apt-get install -yqq \
        # Needed for matplotlib on 3.8.7
        pkg-config \
        libfreetype6 \
        libfreetype6-dev \
        libpng-dev \
    && cd dagster \
    && make install_dev_python_modules \
    && pip freeze --exclude-editable > /snapshot-reqs.txt


FROM "${BASE_IMAGE}"

COPY --from=snapshot_builder /snapshot-reqs.txt .

RUN apt-get update \
    && apt-get install -yqq \
        # Needed for matplotlib on 3.8.7
        pkg-config \
        libfreetype6 \
        libfreetype6-dev \
        libpng-dev \
    && apt-get remove -yqq \
    && apt-get autoremove -yqq --purge \
    && apt-get clean

RUN pip install -r /snapshot-reqs.txt \
    && rm /snapshot-reqs.txt
