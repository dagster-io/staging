import pytest

from dagster.check import CheckError
from dagster.core.queuer.instant_queuer import InstantRunQueuer
from dagster.core.storage.pipeline_run import PipelineRunStatus
from dagster.core.test_utils import create_run_for_test, instance_for_test

from .utils import new_pipeline_run


@pytest.fixture()
def instance():
    overrides = {
        "run_launcher": {"module": "dagster.core.test_utils", "class": "MockedRunLauncher"}
    }
    with instance_for_test(overrides=overrides) as inst:
        yield inst


@pytest.fixture()
def queuer(instance):  # pylint: disable=redefined-outer-name

    q = InstantRunQueuer()
    q.initialize(instance)
    yield q


def test_enqueue_run(instance, queuer):  # pylint: disable=redefined-outer-name
    run = queuer.enqueue_run(
        instance, new_pipeline_run(run_id="foo-1", status=PipelineRunStatus.QUEUED)
    )
    assert run.run_id == "foo-1"

    assert len(instance.run_launcher.queue()) == 1
    assert instance.run_launcher.queue()[0].run_id == "foo-1"
    assert instance.run_launcher.queue()[0].status == PipelineRunStatus.NOT_STARTED
    assert instance.get_run_by_id("foo-1")


def test_enqueue_run_checks_status(instance, queuer):  # pylint: disable=redefined-outer-name
    with pytest.raises(CheckError):
        queuer.enqueue_run(
            instance, new_pipeline_run(run_id="foo-1", status=PipelineRunStatus.NOT_STARTED)
        )


def test_enqueue_run_already_in_run_store(instance, queuer):  # pylint: disable=redefined-outer-name
    create_run_for_test(instance, run_id="foo-1")
    with pytest.raises(CheckError):
        queuer.enqueue_run(
            instance, new_pipeline_run(run_id="foo-1", status=PipelineRunStatus.NOT_STARTED)
        )
