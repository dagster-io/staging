import pytest
from dagster_tests.api_tests.utils import get_foo_pipeline_handle

from dagster import check
from dagster.core.host_representation import ExternalPipeline
from dagster.core.queuing.dequeuer import IN_PROGRESS_STATUSES, DagsterDequeuer
from dagster.core.storage.pipeline_run import PipelineRunStatus
from dagster.core.test_utils import create_run_for_test, instance_for_test


@pytest.fixture
def instance_and_runs(mocker):
    with instance_for_test() as instance:
        launched_runs = set()

        def launch_runs(run_id, external_pipeline):
            check.is_str(run_id)
            check.inst(external_pipeline, ExternalPipeline)
            launched_runs.add(run_id)

        mocker.patch("dagster.core.instance.DagsterInstance.launch_run", side_effect=launch_runs)
        yield instance, launched_runs


def create_run(instance, **kwargs):
    pipeline_handle = get_foo_pipeline_handle()
    create_run_for_test(
        instance, pipeline_origin=pipeline_handle.get_origin(), pipeline_name="foo", **kwargs
    )


def test_attempt_to_launch_runs_filter(instance_and_runs):  # pylint: disable=redefined-outer-name
    instance, launched_runs = instance_and_runs

    create_run(
        instance, run_id="queued-run", status=PipelineRunStatus.QUEUED,
    )

    create_run(
        instance, run_id="non-queued-run", status=PipelineRunStatus.NOT_STARTED,
    )

    dequeuer = DagsterDequeuer(instance)
    dequeuer.attempt_to_launch_runs()

    assert launched_runs == set(["queued-run"])


def test_attempt_to_launch_runs_no_queued(
    instance_and_runs,
):  # pylint: disable=redefined-outer-name
    instance, launched_runs = instance_and_runs

    create_run(
        instance, run_id="queued-run", status=PipelineRunStatus.STARTED,
    )
    create_run(
        instance, run_id="non-queued-run", status=PipelineRunStatus.NOT_STARTED,
    )

    dequeuer = DagsterDequeuer(instance)
    dequeuer.attempt_to_launch_runs()

    assert launched_runs == set()


@pytest.mark.parametrize(
    "num_in_progress_runs", [0, 1, 3, 4, 5],
)
def test_get_queued_runs_max_runs(
    instance_and_runs, num_in_progress_runs
):  # pylint: disable=redefined-outer-name
    instance, launched_runs = instance_and_runs
    max_runs = 4

    # fill run store with ongoing runs
    in_progress_run_ids = ["in_progress-run-{}".format(i) for i in range(num_in_progress_runs)]
    for i, run_id in enumerate(in_progress_run_ids):
        # get a selection of all in progress statuses
        status = IN_PROGRESS_STATUSES[i % len(IN_PROGRESS_STATUSES)]
        create_run(
            instance, run_id=run_id, status=status,
        )

    # add more queued runs than should be launched
    queued_run_ids = ["queued-run-{}".format(i) for i in range(max_runs + 1)]
    for run_id in queued_run_ids:
        create_run(
            instance, run_id=run_id, status=PipelineRunStatus.QUEUED,
        )

    dequeuer = DagsterDequeuer(instance, max_concurrent_runs=max_runs)
    dequeuer.attempt_to_launch_runs()

    assert len(launched_runs) == max(0, max_runs - num_in_progress_runs)
