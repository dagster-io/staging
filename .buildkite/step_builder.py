import os
from enum import Enum

from defines import (
    INTEGRATION_IMAGE_VERSION,
    UNIT_IMAGE_VERSION,
    WINDOWS_IMAGE_VERSION,
    SupportedPythons,
)

TIMEOUT_IN_MIN = 20

DOCKER_PLUGIN = "docker#v3.2.0"
ECR_PLUGIN = "ecr#v2.0.0"


AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID")
AWS_ECR_REGION = "us-west-1"

ECR_SETTINGS = {
    "login": True,
    "no-include-email": True,
    "account-ids": AWS_ACCOUNT_ID,
    "region": AWS_ECR_REGION,
}


def wait_step():
    return "wait"


class BuildkiteQueue(Enum):
    DOCKER = "docker-p"
    MEDIUM = "buildkite-medium-v5-0-1"
    WINDOWS = "buildkite-windows-v5-0-1"

    @classmethod
    def contains(cls, value):
        return isinstance(value, cls)


class StepBuilder:
    def __init__(self, label, key=None):
        self._step = {
            "agents": {"queue": BuildkiteQueue.MEDIUM.value},
            "label": label,
            "timeout_in_minutes": TIMEOUT_IN_MIN,
            "retry": {
                "automatic": [
                    {"exit_status": -1, "limit": 2},  # agent lost
                    {"exit_status": 255, "limit": 2},  # agent forced shut down
                ]
            },
        }
        if key is not None:
            self._step["key"] = key

    def run(self, *argc):
        commands = []
        for entry in argc:
            if isinstance(entry, list):
                commands.extend(entry)
            else:
                commands.append(entry)

        self._step["commands"] = ["time " + cmd for cmd in commands]
        return self

    def on_windows_image(self, ver, env=None):
        # TODO: Move everything up to 3.8.7
        # if ver != SupportedPython.V3_8:
        #     raise Exception("Unsupported python version for Windows image {ver}".format(ver=ver))

        image = "buildkite-windows:py{python_version}-{image_version}".format(
            python_version=ver, image_version=WINDOWS_IMAGE_VERSION
        )

        self._step["plugins"] = [
            {ECR_PLUGIN: ECR_SETTINGS},
            {
                DOCKER_PLUGIN: {
                    "image": "{account_id}.dkr.ecr.us-west-2.amazonaws.com/{image}".format(
                        account_id=AWS_ACCOUNT_ID, image=image
                    ),
                    "environment": ["BUILDKITE"] + (env or []),
                    "always-pull": True,
                    "mount-ssh-agent": True,
                }
            },
        ]
        return self

    def on_python_image(self, image, env=None):
        docker_settings = {
            "image": "{account_id}.dkr.ecr.us-west-1.amazonaws.com/{image}".format(
                account_id=AWS_ACCOUNT_ID, image=image
            ),
            "environment": ["BUILDKITE"] + (env or []),
            "shell": ["/bin/bash", "-xeuc"],
            "always-pull": True,
            "mount-ssh-agent": True,
            "network": "kind",
            "volumes": ["/var/run/docker.sock:/var/run/docker.sock"],
        }

        self._step["plugins"] = [{ECR_PLUGIN: ECR_SETTINGS}, {DOCKER_PLUGIN: docker_settings}]
        return self

    def on_unit_image(self, ver, env=None):
        if ver not in SupportedPythons:
            raise Exception("Unsupported python version for unit image {ver}".format(ver=ver))

        return self.on_python_image(
            image="buildkite-unit:py{python_version}-{image_version}".format(
                python_version=ver, image_version=UNIT_IMAGE_VERSION
            ),
            env=env,
        )

    def on_integration_image(self, ver, env=None):
        if ver not in SupportedPythons:
            raise Exception(
                "Unsupported python version for integration image {ver}".format(ver=ver)
            )

        return self.on_python_image(
            image="buildkite-integration:py{python_version}-{image_version}".format(
                python_version=ver, image_version=INTEGRATION_IMAGE_VERSION
            ),
            env=env,
        )

    def with_timeout(self, num_minutes):
        self._step["timeout_in_minutes"] = num_minutes
        return self

    def with_retry(self, num_retries):
        self._step["retry"] = {"automatic": {"limit": num_retries}}
        return self

    def on_queue(self, queue):
        assert BuildkiteQueue.contains(queue)

        self._step["agents"]["queue"] = queue.value
        return self

    def depends_on(self, step_keys):
        self._step["depends_on"] = step_keys
        return self

    def build(self):
        return self._step
