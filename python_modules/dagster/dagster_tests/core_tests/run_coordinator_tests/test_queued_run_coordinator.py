import pytest
from dagster.check import CheckError
from dagster.core.errors import DagsterInvalidConfigError
from dagster.core.run_coordinator.queued_run_coordinator import QueuedRunCoordinator
from dagster.core.storage.pipeline_run import PipelineRunStatus
from dagster.core.test_utils import create_run_for_test, environ, instance_for_test
from dagster.utils import merge_dicts
from dagster_tests.api_tests.utils import get_foo_external_pipeline
from dagster_tests.core_tests.run_coordinator_tests.utils import TestRunCoordinator


class TestQueuedRunCoordinator(TestRunCoordinator):
    __test__ = True

    @pytest.fixture(name="coordinator", scope="function")
    def coordinator_fixture(self, instance):  # pylint: disable=arguments-differ
        coordinator = QueuedRunCoordinator()
        coordinator.register_instance(instance)
        yield coordinator

    def test_config(self):
        with environ({"MAX_RUNS": "10", "DEQUEUE_INTERVAL": "7"}):
            with instance_for_test(
                overrides={
                    "run_coordinator": {
                        "module": "dagster.core.run_coordinator",
                        "class": "QueuedRunCoordinator",
                        "config": {
                            "max_concurrent_runs": {
                                "env": "MAX_RUNS",
                            },
                            "tag_concurrency_limits": [
                                {"key": "foo", "value": "bar", "limit": 3},
                                {"key": "backfill", "limit": 2},
                            ],
                            "dequeue_interval_seconds": {
                                "env": "DEQUEUE_INTERVAL",
                            },
                        },
                    }
                }
            ) as _:
                pass

        with pytest.raises(DagsterInvalidConfigError):
            with instance_for_test(
                overrides={
                    "run_coordinator": {
                        "module": "dagster.core.run_coordinator",
                        "class": "QueuedRunCoordinator",
                        "config": {
                            "tag_concurrency_limits": [
                                {"key": "backfill"},
                            ],
                        },
                    }
                }
            ) as _:
                pass

    def test_submit_run(self, instance, coordinator):
        with get_foo_external_pipeline() as external_pipeline:
            run = self.create_run(
                instance, external_pipeline, run_id="foo-1", status=PipelineRunStatus.NOT_STARTED
            )
            returned_run = coordinator.submit_run(run, external_pipeline)
            assert returned_run.run_id == "foo-1"
            assert returned_run.status == PipelineRunStatus.QUEUED

            assert len(instance.run_launcher.queue()) == 0
            stored_run = instance.get_run_by_id("foo-1")
            assert stored_run.status == PipelineRunStatus.QUEUED

    def test_submit_run_checks_status(  # pylint: disable=arguments-differ
        self, instance, coordinator
    ):
        super().test_submit_run_checks_status(
            instance, coordinator, created_run_status=PipelineRunStatus.QUEUED
        )

    def test_cancel_run(self, instance, coordinator):
        with get_foo_external_pipeline() as external_pipeline:
            run = self.create_run(
                instance, external_pipeline, run_id="foo-1", status=PipelineRunStatus.NOT_STARTED
            )
            assert not coordinator.can_cancel_run(run.run_id)

            coordinator.submit_run(run, external_pipeline)
            assert coordinator.can_cancel_run(run.run_id)

            coordinator.cancel_run(run.run_id)
            stored_run = instance.get_run_by_id("foo-1")
            assert stored_run.status == PipelineRunStatus.CANCELED
            assert not coordinator.can_cancel_run(run.run_id)
