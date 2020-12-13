import subprocess

import yaml

ECR = "968703565975.dkr.ecr.us-west-1.amazonaws.com"

DAGSTER_VERSION = (
    str(subprocess.check_output(["dagster", "--version"]), encoding="utf-8")
    .strip("\n")
    .split(" ")[-1]
)


def rebuild_unit_images():
    return [
        f"aws ecr get-login-password --region us-west-1 | docker login --username AWS --password-stdin {ECR}",
        f"dagster-image build-all --name buildkite-unit-snapshot-builder --dagster-version {DAGSTER_VERSION}",
        "dagster-image snapshot -t unit",
        "dagster-image build-all --name buildkite-unit --dagster-version {DAGSTER_VERSION}",
        "dagster-image push-all --name buildkite-unit",
        # FIXME update the Dockerfile in dagster-test
        # FIXME update UNIT_IMAGE_VERSION
    ]


# FIXME need to pass the git hash into the Dockerfiles
def rebuild_integration_images():
    return [
        f"aws ecr get-login-password --region us-west-1 | docker login --username AWS --password-stdin {ECR}",
        f"dagster-image build-all --name buildkite-integration-snapshot-builder --dagster-version {DAGSTER_VERSION}",
        f"dagster-image snapshot -t integration",
        f"dagster-image build-all --name buildkite-integration --dagster-version {DAGSTER_VERSION}",
        f"dagster-image push-all --name buildkite-integration",
        # FIXME update INTEGRATION_IMAGE_VERSION
        # FIXME push the diff
    ]


def rebuild_steps():
    return rebuild_unit_images() + rebuild_integration_images()


if __name__ == "__main__":
    print(  # pylint: disable=print-call
        yaml.dump(
            {
                "env": {
                    "CI_NAME": "buildkite",
                    "CI_BUILD_NUMBER": "$BUILDKITE_BUILD_NUMBER",
                    "CI_BUILD_URL": "$BUILDKITE_BUILD_URL",
                    "CI_BRANCH": "$BUILDKITE_BRANCH",
                    "CI_PULL_REQUEST": "$BUILDKITE_PULL_REQUEST",
                },
                "steps": rebuild_steps(),
            },
            default_flow_style=False,
        )
    )
