import pytest
from dagster_tests.api_tests.utils import get_foo_pipeline_handle

from dagster.check import CheckError
from dagster.core.runs_coordinator.queued_runs_coordinator import QueuedRunsCoordinator
from dagster.core.storage.pipeline_run import PipelineRunStatus
from dagster.core.test_utils import create_run_for_test, instance_for_test
from dagster.utils import merge_dicts


@pytest.fixture()
def instance():
    overrides = {
        "run_launcher": {"module": "dagster.core.test_utils", "class": "MockedRunLauncher"}
    }
    with instance_for_test(overrides=overrides) as inst:
        yield inst


@pytest.fixture()
def coordinator(instance):  # pylint: disable=redefined-outer-name
    run_coordinator = QueuedRunsCoordinator()
    run_coordinator.initialize(instance)
    yield run_coordinator


def create_run(instance, **kwargs):  # pylint: disable=redefined-outer-name
    pipeline_handle = get_foo_pipeline_handle()
    pipeline_args = merge_dicts(
        {"pipeline_name": "foo", "pipeline_origin": pipeline_handle.get_origin(),}, kwargs,
    )
    return create_run_for_test(instance, **pipeline_args)


def test_submit_run(instance, coordinator):  # pylint: disable=redefined-outer-name
    run = coordinator.submit_run(
        create_run(instance, run_id="foo-1", status=PipelineRunStatus.NOT_STARTED), None
    )
    assert run.run_id == "foo-1"

    assert len(instance.run_launcher.queue()) == 0
    stored_run = instance.get_run_by_id("foo-1")
    assert stored_run.status == PipelineRunStatus.QUEUED


def test_submit_run_checks_status(instance, coordinator):  # pylint: disable=redefined-outer-name
    with pytest.raises(CheckError):
        coordinator.submit_run(
            create_run(instance, run_id="foo-1", status=PipelineRunStatus.QUEUED), None,
        )
