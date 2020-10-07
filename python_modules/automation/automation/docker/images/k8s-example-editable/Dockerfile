ARG BASE_IMAGE
FROM "${BASE_IMAGE}"

ARG DAGSTER_VERSION

RUN apt-get update -yqq && \
    apt-get install -yqq cron wget

RUN GRPC_HEALTH_PROBE_VERSION=v0.2.1 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

COPY build_cache/ /

RUN pip install \
    -e dagster \
    -e dagster-postgres \
    -e dagster-cron \
    -e dagster-celery[flower,redis,kubernetes] \
    -e dagster-aws \
    -e dagster-k8s \
    -e dagster-celery-k8s
