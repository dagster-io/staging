"""
This test incorporates some operational flexibility scenario. 

I have code that is deployed that I do not to change, but that I do want to
operate flexibly.

In the "normal" case I want all files written in a regularized pattern that
I control.

However things frequently go wrong in this process because there are manual steps
involved (there is a counterparty which manually creates these files and drops
them into an sftp server.) In order to debug that process they often drop
test files in random locations that are not in the folder that the sensor
listens to.

Additionally this is a pipeline I've inherited and I haven't set up a bunch of
local testing and mocking. I also cannot write against the python API because
1) I can't shell into the deployed machine and 2) I cannot access cloud resources
from my laptop. Therefore my debugging flow is to redeploy code and the run
ad-hoc executions against the deployed code in a staging instance. 

I want to be able to "do it live" in production and debug things on the
prod instance by modifying config.

"""

from dagster import execute_pipeline, pipeline, solid

# note that this pipeline doesn't reflect the english above. had to get the baby


@solid
def initial_ingest(_, external_s3_path: str) -> str:
    assert external_s3_path
    return "initial"


@solid
def process_step_one(_, s3_path) -> str:
    # read from s3, do some compute, write to s3
    assert s3_path
    return "step_one"


@solid
def process_step_two(_, s3_path) -> str:
    assert s3_path
    # read from s3, do some compute, write to s3
    return "step_two"


@pipeline
def pipe():
    process_step_two(process_step_one(initial_ingest()))


def test_basic_pipeline():
    result = execute_pipeline(
        pipe, run_config={"solids": {"initial_ingest": {"inputs": {"external_s3_path": "dummy"}}}}
    )

    assert result.success
