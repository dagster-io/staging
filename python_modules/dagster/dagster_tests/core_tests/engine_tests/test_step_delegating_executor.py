import subprocess
from typing import List

import pytest
from dagster import executor, pipeline, reconstructable, solid
from dagster.core.definitions.executor import multiple_process_executor_requirements
from dagster.core.definitions.mode import ModeDefinition
from dagster.core.events import DagsterEvent
from dagster.core.execution.api import execute_pipeline
from dagster.core.execution.retries import RetryMode
from dagster.core.executor.step_delegating import StepDelegatingExecutor, StepHandler
from dagster.core.executor.step_delegating.step_delegating_executor import StepIsolationMode
from dagster.core.storage.fs_io_manager import fs_io_manager
from dagster.core.test_utils import instance_for_test
from dagster.serdes import serialize_dagster_namedtuple


class TestStepHandler(StepHandler):
    @property
    def name(self):
        return "TestStepHandler"

    def launch_step(self, execute_step_args):
        print("TestStepHandler Launching Step!")  # pylint: disable=print-call
        subprocess.Popen(
            ["dagster", "api", "execute_step", serialize_dagster_namedtuple(execute_step_args)]
        )

    def check_step_health(self, _execute_step_args) -> List[DagsterEvent]:
        return []

    def terminate_step(self, execute_step_args):
        raise NotImplementedError()


@executor(
    name="test_step_delegating_executor",
    config_schema={"isolation_mode": str},
    requirements=multiple_process_executor_requirements(),
)
def test_step_delegating_executor(init_ctx):
    return StepDelegatingExecutor(
        TestStepHandler(retries=RetryMode.DISABLED),
        step_isolation=StepIsolationMode(init_ctx.executor_config.get("isolation_mode")),
    )


@solid
def bar_solid(_):
    return "bar"


@solid
def baz_solid(_, bar):
    return bar * 2


@pipeline(
    mode_defs=[
        ModeDefinition(
            executor_defs=[test_step_delegating_executor],
            resource_defs={"io_manager": fs_io_manager},
        )
    ]
)
def foo_pipline():
    baz_solid(bar_solid())
    bar_solid()


@pytest.mark.parametrize("isolation_mode", StepIsolationMode)
def test_execute(isolation_mode):
    with instance_for_test() as instance:
        result = execute_pipeline(
            reconstructable(foo_pipline),
            instance=instance,
            run_config={
                "execution": {
                    "test_step_delegating_executor": {
                        "config": {"isolation_mode": isolation_mode.value}
                    }
                }
            },
        )
    assert any(
        [
            "Starting execution with step handler TestStepHandler" in event
            for event in result.event_list
        ]
    )
    assert any(["STEP_START" in event for event in result.event_list])
    assert result.success
