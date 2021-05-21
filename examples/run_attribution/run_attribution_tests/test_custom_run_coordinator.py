import pytest
from dagster.core.storage.pipeline_run import PipelineRunStatus
from dagster_tests.api_tests.utils import get_foo_external_pipeline
from dagster_tests.core_tests.run_coordinator_tests.test_queued_run_coordinator import (
    TestQueuedRunCoordinator,
)
from mock import patch
from run_attribution.custom_run_coordinator import CustomRunCoordinator


class TestCustomRunCoordinator(TestQueuedRunCoordinator):
    __test__ = True

    @pytest.fixture(name="coordinator", scope="function")
    def coordinator_fixture(self, instance):  # pylint: disable=arguments-differ
        coordinator = CustomRunCoordinator()
        coordinator.register_instance(instance)
        yield coordinator

    def test_cancel_run(self, instance, coordinator):
        with pytest.warns(UserWarning, match="Couldn't decode session token"):
            super().test_cancel_run(instance, coordinator)

    def test_submit_run_checks_status(self, instance, coordinator):
        with pytest.warns(UserWarning, match="Couldn't decode session token"):
            super().test_submit_run_checks_status(instance, coordinator)

    def test_submit_run(self, instance, coordinator):
        with pytest.warns(UserWarning, match="Couldn't decode session token"):
            super().test_submit_run(instance, coordinator)

    def test_session_header_decode_failure(self, instance, coordinator):
        run_id = "foo-1"
        with get_foo_external_pipeline() as external_pipeline:
            with patch(
                "run_attribution.custom_run_coordinator.has_request_context"
            ) as mock_has_request_context:
                with patch("run_attribution.custom_run_coordinator.warnings") as mock_warnings:
                    mock_has_request_context.return_value = False

                    run = self.create_run(
                        instance,
                        external_pipeline,
                        run_id=run_id,
                        status=PipelineRunStatus.NOT_STARTED,
                    )
                    returned_run = coordinator.submit_run(run, external_pipeline)

                    assert returned_run.run_id == run_id
                    assert returned_run.status == PipelineRunStatus.QUEUED
                    tags = instance.get_run_tags()
                    assert len(tags) == 0
                    mock_warnings.warn.assert_called_once()
                    assert mock_warnings.warn.call_args.args[0].startswith(
                        "Couldn't decode session token"
                    )

    def test_session_header_decode_success(self, instance, coordinator):
        run_id, test_session_token = "foo", "bar"
        with get_foo_external_pipeline() as external_pipeline:
            with patch(
                "run_attribution.custom_run_coordinator.has_request_context"
            ) as mock_has_request_context:
                with patch(
                    "run_attribution.custom_run_coordinator.request",
                    headers={"Dagster-Session-Token": test_session_token},
                ):
                    mock_has_request_context.return_value = True

                    run = self.create_run(
                        instance,
                        external_pipeline,
                        run_id=run_id,
                        status=PipelineRunStatus.NOT_STARTED,
                    )
                    returned_run = coordinator.submit_run(run, external_pipeline)

                    assert returned_run.run_id == run_id
                    assert returned_run.status == PipelineRunStatus.QUEUED
                    tags = instance.get_run_tags()
                    assert len(tags) == 1
                    (tag_name, set_of_tag_values) = tags[0]
                    assert tag_name == "user"
                    assert set_of_tag_values == {test_session_token}
