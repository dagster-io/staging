import pytest
from dagster_tests.api_tests.utils import get_foo_pipeline_handle

from dagster.core.queuer.instant_queuer import InstantQueuer
from dagster.core.storage.pipeline_run import PipelineRunStatus
from dagster.core.test_utils import instance_for_test
from dagster.utils import merge_dicts


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


def get_run_args(**kwargs):  # pylint: disable=redefined-outer-name

    pipeline_handle = get_foo_pipeline_handle()
    return merge_dicts(
        {
            "pipeline_origin": pipeline_handle.get_origin(),
            "pipeline_name": "foo",
            "run_id": None,
            "run_config": None,
            "mode": None,
            "solids_to_execute": None,
            "step_keys_to_execute": None,
            "tags": None,
            "root_run_id": None,
            "parent_run_id": None,
            "pipeline_snapshot": None,
            "execution_plan_snapshot": None,
            "parent_pipeline_snapshot": None,
        },
        kwargs,
    )


def test_enqueue_run(instance, queuer):  # pylint: disable=redefined-outer-name

    queuer.enqueue_run(instance, **get_run_args(run_id="foo-1"))
    assert len(instance.run_launcher.queue()) == 1
    assert instance.run_launcher.queue()[0].run_id == "foo-1"
    assert instance.run_launcher.queue()[0].status == PipelineRunStatus.NOT_STARTED
