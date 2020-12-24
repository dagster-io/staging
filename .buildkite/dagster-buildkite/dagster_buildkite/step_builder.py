from .defines import (
    AWS_ACCOUNT_ID,
    AWS_ECR_REGION,
    DOCKER_PLUGIN,
    ECR_PLUGIN,
    TIMEOUT_IN_MIN,
    BuildkiteQueue,
    SupportedPython,
    SupportedPythons,
)
from .images.versions import (
    INTEGRATION_IMAGE_VERSION,
    UNIT_IMAGE_VERSION,
    WINDOWS_INTEGRATION_IMAGE_VERSION,
)

DOCKER_SETTINGS = {
    "shell": ["/bin/bash", "-xeuc"],
    "always-pull": True,
    "mount-ssh-agent": True,
    "volumes": ["/var/run/docker.sock:/var/run/docker.sock"],
    "network": "kind",
}

ECR_SETTINGS = {
    "login": True,
    "no-include-email": True,
    "account-ids": AWS_ACCOUNT_ID,
    "region": AWS_ECR_REGION,
}


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
        self._step["commands"] = list(argc)
        return self

    def on_python_image(self, image, env=None):
        docker_settings = {
            "image": f"{AWS_ACCOUNT_ID}.dkr.ecr.us-west-2.amazonaws.com/{image}",
            "environment": ["BUILDKITE"] + (env or []),
            **DOCKER_SETTINGS,
        }

        self._step["plugins"] = [{ECR_PLUGIN: ECR_SETTINGS}, {DOCKER_PLUGIN: docker_settings}]
        return self

    def on_unit_image(self, python_version: str, env=None):
        if python_version not in SupportedPythons:
            raise Exception(f"Unsupported python version for unit image {python_version}")

        return self.on_python_image(f"buildkite-unit:py{python_version}-{UNIT_IMAGE_VERSION}", env)

    def on_integration_image(self, python_version: str, env=None):
        if python_version not in SupportedPythons:
            raise Exception(f"Unsupported python version for integration image {python_version}")

        return self.on_python_image(
            f"buildkite-integration:py{python_version}-{INTEGRATION_IMAGE_VERSION}", env,
        )

    def on_windows_image(self, python_version: str, env=None):
        if python_version != SupportedPython.V3_8:
            raise Exception(
                f"Unsupported python version for windows integration image {python_version}"
            )

        image = (
            f"buildkite-integration-windows:py{python_version}-{WINDOWS_INTEGRATION_IMAGE_VERSION}",
        )

        docker_settings = {
            "image": f"{AWS_ACCOUNT_ID}.dkr.ecr.us-west-2.amazonaws.com/{image}",
            "environment": ["BUILDKITE"] + (env or []),
            "volumes": ["/var/run/docker.sock:/var/run/docker.sock"],
            "network": "kind",
            "always-pull": True,
        }

        self._step["plugins"] = [{ECR_PLUGIN: ECR_SETTINGS}, {DOCKER_PLUGIN: docker_settings}]
        return self.on_queue(BuildkiteQueue.WINDOWS)

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
