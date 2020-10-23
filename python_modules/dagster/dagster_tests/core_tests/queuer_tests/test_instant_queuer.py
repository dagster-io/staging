import pytest
from dagster_tests.api_tests.utils import get_foo_pipeline_handle

from dagster.core.queuer.instant_queuer import InstantQueuer
from dagster.core.storage.pipeline_run import PipelineRunStatus
from dagster.core.test_utils import create_run_for_test, instance_for_test


@pytest.fixture()
def instance():
    overrides = {"run_launcher": {"module": "dagster_tests.utils", "class": "InMemoryRunLauncher",}}
    with instance_for_test(overrides=overrides) as inst:
        yield inst


@pytest.fixture()
def queuer(instance):  # pylint: disable=redefined-outer-name

    q = InstantQueuer()
    q.initialize(instance)
    yield q


def create_run(instance, **kwargs):  # pylint: disable=redefined-outer-name

    pipeline_handle = get_foo_pipeline_handle()
    create_run_for_test(
        instance=instance,
        pipeline_origin=pipeline_handle.get_origin(),
        pipeline_name="foo",
        **kwargs
    )


def test_enqueue_run(instance, queuer):  # pylint: disable=redefined-outer-name

    queuer.enqueue_run(
        instance, run=create_run(instance, run_id="foo-1",),
    )
    assert len(instance.run_launcher.queue()) == 1
    assert instance.run_launcher.queue()[0].run_id == "foo-1"
    assert instance.run_launcher.queue()[0].status == PipelineRunStatus.NOT_STARTED
