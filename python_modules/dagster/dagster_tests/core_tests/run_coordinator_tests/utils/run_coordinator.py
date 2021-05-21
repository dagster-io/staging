import pytest
from dagster.check import CheckError
from dagster.core.storage.pipeline_run import PipelineRunStatus
from dagster.core.test_utils import create_run_for_test, instance_for_test
from dagster.utils import merge_dicts
from dagster_tests.api_tests.utils import get_foo_external_pipeline


class TestRunCoordinator:
    """
    You can extend this class to easily run these set of tests on any run coodinator. When extending,
    you simply need to override the `coordinator_fixture` fixture and return your implementation of
    a `RunCoordinator`.

    For example:

    ```
    class TestMyRunCoordinator(TestRunCoordinator):
        __test__ = True

        @pytest.fixture(scope="function", name="coordinator")
        def coordinator_fixture(self, instance):  # pylint: disable=arguments-differ
            run_coordinator = MyRunCoordinator()
            run_coordinator.register_instance(instance)
            yield run_coordinator
    ```
    """

    __test__ = False

    @pytest.fixture()
    def instance(self):
        overrides = {
            "run_launcher": {"module": "dagster.core.test_utils", "class": "MockedRunLauncher"}
        }
        with instance_for_test(overrides=overrides) as inst:
            yield inst

    @pytest.fixture(name="coordinator", params=[])
    def coordinator_fixture(self, request):
        with request.param() as s:
            yield s

    def create_run(self, instance, external_pipeline, **kwargs):
        pipeline_args = merge_dicts(
            {
                "pipeline_name": "foo",
                "external_pipeline_origin": external_pipeline.get_external_origin(),
            },
            kwargs,
        )
        return create_run_for_test(instance, **pipeline_args)

    def test_submit_run_checks_status(
        self, instance, coordinator, created_run_status=PipelineRunStatus.STARTED
    ):
        with get_foo_external_pipeline() as external_pipeline:
            run = self.create_run(
                instance, external_pipeline, run_id="foo-1", status=created_run_status
            )
            with pytest.raises(CheckError):
                coordinator.submit_run(run, external_pipeline)
