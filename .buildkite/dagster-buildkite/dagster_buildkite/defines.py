import os
from enum import Enum


class SupportedPython:
    V3_8 = "3.8.7"
    V3_7 = "3.7.9"
    V3_6 = "3.6.12"


class BuildkiteQueue(Enum):
    DOCKER = "buildkite-docker-v5-0-1"
    MEDIUM = "buildkite-medium-v5-0-1"
    WINDOWS = "buildkite-windows-v5-0-1"

    @classmethod
    def contains(cls, value):
        return isinstance(value, cls)


SupportedPythons = [
    SupportedPython.V3_6,
    SupportedPython.V3_7,
    SupportedPython.V3_8,
]


TOX_MAP = {
    SupportedPython.V3_8: "py38",
    SupportedPython.V3_7: "py37",
    SupportedPython.V3_6: "py36",
}

# Default build step timeout
TIMEOUT_IN_MIN = 20

# Versions of Docker and ECR plugins to use
DOCKER_PLUGIN = "docker#v3.7.0"
ECR_PLUGIN = "ecr#v2.2.0"

AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID")
AWS_ECR_REGION = "us-west-2"
AWS_ECR_URL = f"{AWS_ACCOUNT_ID}.dkr.ecr.{AWS_ECR_REGION}.amazonaws.com"

# https://github.com/dagster-io/dagster/issues/1662
DO_COVERAGE = True

# GCP tests need appropriate credentials
GCP_CREDS_LOCAL_FILE = "/tmp/gcp-key-elementl-dev.json"

IS_WINDOWS = os.name == "nt"
