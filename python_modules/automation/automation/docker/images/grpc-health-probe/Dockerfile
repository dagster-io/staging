ARG BASE_IMAGE
FROM "${BASE_IMAGE}"

RUN apt-get update -yqq && \
    apt-get install -yqq wget

# Get install script
COPY build_cache/ /

RUN chmod +x /install-grpc-health-check-probe.sh && /install-grpc-health-check-probe.sh
