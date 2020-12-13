####################################################################################################
#
# DAGSTER INTEGRATION IMAGE
#
# We use this Dockerfile to build an image for our Buildkite CI/CD pipeline.
#
####################################################################################################

ARG BASE_IMAGE
ARG PYTHON_VERSION
FROM "${BASE_IMAGE}"

ADD snapshot-reqs-$PYTHON_VERSION.txt /snapshot-reqs.txt

# We need Cython and numpy to build pandas from source on 3.9
RUN if [ "$PYTHON_VERSION" = "3.9.1" ] ; then pip install Cython==0.29.21 numpy==1.18.5; fi

# Using the legacy resolver until we can get off pip
RUN pip install --use-deprecated=legacy-resolver -r /snapshot-reqs.txt \
    && rm /snapshot-reqs.txt
