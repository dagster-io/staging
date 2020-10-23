import pytest
from dagster_tests.api_tests.utils import get_foo_pipeline_handle
from dagster_tests.core_tests.storage_tests import FakePipelineOrigin
from dagster_tests.utils import InMemoryRunLauncher

from dagster.core.queuer.instant_queuer import InstantQueuer
from dagster.core.test_utils import create_run_for_test, instance_for_test


@pytest.fixture()
def instance():
    overrides = {"run_launcher": {"module": "dagster_tests.utils", "class": "InMemoryRunLauncher",}}
    with instance_for_test(overrides=overrides) as instance:
        yield instance


@pytest.fixture()
def queuer(instance):
    queuer = InstantQueuer()
    queuer.initialize(instance)
    yield queuer


def create_run(instance, **kwargs):
    pipeline_handle = get_foo_pipeline_handle()
    create_run_for_test(
        instance, pipeline_origin=pipeline_handle.get_origin(), pipeline_name="foo", **kwargs
    )


def test_enqueue_run(instance, queuer):
    queuer.enqueue_run(
        instance, create_run(run_id="foo-1",),
    )
    assert len(instance.run_launcher.queue()) == 1
    assert instance.run_launcher.queue()[0].run_id == "foo-1"
