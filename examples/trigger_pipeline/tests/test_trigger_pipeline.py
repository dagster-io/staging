# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
import subprocess
import time

import pytest
from dagster import execute_pipeline
from trigger_pipeline.trigger_pipeline.repo import do_math
from trigger_pipeline.trigger_pipeline.trigger import launch_pipeline_over_graphql


def test_trigger_do_math_pipeline():
    run_config = {"solids": {"add_one": {"inputs": {"num": 5}}, "add_two": {"inputs": {"num": 6}}}}
    res = execute_pipeline(do_math, run_config=run_config)
    assert res.success
    assert res.result_for_solid("subtract").output_value() == -2


@pytest.fixture
def dagit_service_up(scope="module"):
    proc = subprocess.Popen(["dagit", "-f", "../trigger_pipeline/repo.py"])
    yield
    proc.kill()


def test_trigger_pipeline_by_gql(dagit_service_up):
    time.sleep(5)
    REPOSITORY_LOCATION = "repo.py"
    REPOSITORY_NAME = "my_repo"
    PIPELINE_NAME = "do_math"
    RUN_CONFIG = {"solids": {"add_one": {"inputs": {"num": 5}}, "add_two": {"inputs": {"num": 6}}}}
    MODE = "default"
    result = launch_pipeline_over_graphql(
        location=REPOSITORY_LOCATION,
        repo_name=REPOSITORY_NAME,
        pipeline_name=PIPELINE_NAME,
        run_config=RUN_CONFIG,
        mode=MODE,
    )
    assert result["launchPipelineExecution"]["__typename"] == "LaunchPipelineRunSuccess"
