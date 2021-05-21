import pytest
from dagster.core.run_coordinator.default_run_coordinator import DefaultRunCoordinator
from dagster.core.storage.pipeline_run import PipelineRunStatus
from dagster_tests.api_tests.utils import get_foo_external_pipeline
from dagster_tests.core_tests.run_coordinator_tests.utils import TestRunCoordinator


class TestDefaultRunCoordinator(TestRunCoordinator):
    __test__ = True

    @pytest.fixture(name="coordinator", scope="function")
    def coordinator_fixture(self, instance):  # pylint: disable=arguments-differ
        coordinator = DefaultRunCoordinator()
        coordinator.register_instance(instance)
        yield coordinator

    def test_submit_run(self, instance, coordinator):
        with get_foo_external_pipeline() as external_pipeline:
            run = self.create_run(instance, external_pipeline, run_id="foo-1")
            returned_run = coordinator.submit_run(run, external_pipeline)
            assert returned_run.run_id == "foo-1"
            assert returned_run.status == PipelineRunStatus.STARTING

            assert len(instance.run_launcher.queue()) == 1
            assert instance.run_launcher.queue()[0].run_id == "foo-1"
            assert instance.run_launcher.queue()[0].status == PipelineRunStatus.STARTING
            assert instance.get_run_by_id("foo-1")
