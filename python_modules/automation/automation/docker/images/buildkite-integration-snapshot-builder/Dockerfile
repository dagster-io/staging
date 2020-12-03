ARG BASE_IMAGE
FROM "${BASE_IMAGE}"

# Last git commit in dagster that build the snapshot files
RUN git clone https://github.com/dagster-io/dagster.git dagster \
    && cd dagster \
    && git checkout 45e4c12154084f225d21eec181ce6c60b95e1c0b

# for some reason this is only failing in the 3.8 image
# RUN cd dagster && git checkout "${SNAPSHOT_GIT_HASH}"

RUN cd dagster && make install_dev_python_modules_verbose
