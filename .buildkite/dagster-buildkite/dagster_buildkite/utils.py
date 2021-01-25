import os
import subprocess
from typing import List

import yaml

DAGIT_PATH = "js_modules/dagit"


def buildkite_yaml_for_steps(steps: list) -> str:
    return yaml.dump(
        {
            "env": {
                "CI_NAME": "buildkite",
                "CI_BUILD_NUMBER": "$BUILDKITE_BUILD_NUMBER",
                "CI_BUILD_URL": "$BUILDKITE_BUILD_URL",
                "CI_BRANCH": "$BUILDKITE_BRANCH",
                "CI_PULL_REQUEST": "$BUILDKITE_PULL_REQUEST",
            },
            "steps": steps,
        },
        default_flow_style=False,
    )


def check_for_release() -> bool:
    try:
        git_tag = str(
            subprocess.check_output(
                ["git", "describe", "--exact-match", "--abbrev=0"], stderr=subprocess.STDOUT
            )
        ).strip("'b\\n")
    except subprocess.CalledProcessError:
        return False

    version = {}
    with open("python_modules/dagster/dagster/version.py") as fp:
        exec(fp.read(), version)  # pylint: disable=W0122

    if git_tag == version["__version__"]:
        return True

    return False


def is_phab_and_dagit_only() -> bool:
    branch_name = os.getenv("BUILDKITE_BRANCH")
    if branch_name is None:
        branch_name = (
            subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"])
            .decode("utf-8")
            .strip()
        )
    if not branch_name.startswith("phabricator"):
        return False

    try:
        base_branch = branch_name.replace("/diff/", "/base/")
        subprocess.check_call(["git", "fetch", "--tags"])
        diff_files = (
            subprocess.check_output(["git", "diff", base_branch, branch_name, "--name-only"])
            .decode("utf-8")
            .strip()
            .split("\n")
        )
        return all(filepath.startswith(DAGIT_PATH) for (filepath) in diff_files)

    except subprocess.CalledProcessError:
        return False


def network_buildkite_container(network_name: str) -> List[str]:
    return [
        # hold onto your hats, this is docker networking at its best. First, we figure out
        # the name of the currently running container...
        "export CONTAINER_ID=`cut -c9- < /proc/1/cpuset`",
        r'export CONTAINER_NAME=`docker ps --filter "id=\${CONTAINER_ID}" --format "{{.Names}}"`',
        # then, we dynamically bind this container into the user-defined bridge
        # network to make the target containers visible...
        f"docker network connect {network_name} \\${{CONTAINER_NAME}}",
    ]


def connect_sibling_docker_container(
    network_name: str, container_name: str, env_variable: str
) -> List[str]:
    return [
        # Now, we grab the IP address of the target container from within the target
        # bridge network and export it; this will let the tox tests talk to the target cot.
        (
            f"export {env_variable}=`docker inspect --format "
            f"'{{{{ .NetworkSettings.Networks.{network_name}.IPAddress }}}}' "
            f"{container_name}`"
        )
    ]
