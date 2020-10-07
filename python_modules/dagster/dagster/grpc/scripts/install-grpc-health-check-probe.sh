#!/bin/bash

GRPC_HEALTH_PROBE_VERSION=v0.2.1 && \
wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
chmod +x /bin/grpc_health_probe


api grpc -h 0.0.0.0 -p 3030 -f /example_project/example_repo/repo.py