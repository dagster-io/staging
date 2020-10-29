import pytest
from dagster_tests.api_tests.utils import get_foo_pipeline_handle

from dagster.check import CheckError
from dagster.core.runs_coordinator.launch_immediate_runs_coordinator import (
    LaunchImmediateRunsCoordinator,
)
from dagster.core.storage.pipeline_run import PipelineRunStatus
from dagster.core.test_utils import create_run_for_test, instance_for_test
from dagster.utils import merge_dicts
from dagster.utils.external import external_pipeline_from_run


@pytest.fixture()
def instance():
    overrides = {
        "run_launcher": {"module": "dagster.core.test_utils", "class": "MockedRunLauncher"}
    }
    with instance_for_test(overrides=overrides) as inst:
        yield inst


@pytest.fixture()
def coodinator(instance):  # pylint: disable=redefined-outer-name
    q = LaunchImmediateRunsCoordinator()
    q.initialize(instance)
    yield q


def call_submit_run(instance, coodinator, run):  # pylint: disable=redefined-outer-name
    with external_pipeline_from_run(instance, run) as external:
        return coodinator.submit_run(instance, run, external)


def create_run(instance, **kwargs):  # pylint: disable=redefined-outer-name
    pipeline_handle = get_foo_pipeline_handle()
    pipeline_args = merge_dicts(
        {"pipeline_name": "foo", "pipeline_origin": pipeline_handle.get_origin(),}, kwargs,
    )
    return create_run_for_test(instance, **pipeline_args)


def test_submit_run(instance, coodinator):  # pylint: disable=redefined-outer-name
    run = call_submit_run(instance, coodinator, create_run(instance, run_id="foo-1"))
    assert run.run_id == "foo-1"

    assert len(instance.run_launcher.queue()) == 1
    assert instance.run_launcher.queue()[0].run_id == "foo-1"
    assert instance.run_launcher.queue()[0].status == PipelineRunStatus.NOT_STARTED
    assert instance.get_run_by_id("foo-1")


def test_submit_run_checks_status(instance, coodinator):  # pylint: disable=redefined-outer-name
    with pytest.raises(CheckError):
        call_submit_run(
            instance,
            coodinator,
            create_run(instance, run_id="foo-1", status=PipelineRunStatus.STARTED),
        )
