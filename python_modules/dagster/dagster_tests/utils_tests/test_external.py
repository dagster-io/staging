from dagster_tests.api_tests.utils import get_foo_pipeline_handle

from dagster.core.test_utils import create_run_for_test, instance_for_test
from dagster.utils.external import external_pipeline_from_run


def test_get_external_pipeline_from_run():
    with instance_for_test() as instance:
        pipeline_handle = get_foo_pipeline_handle()
        run = create_run_for_test(
            instance,
            pipeline_name=pipeline_handle.pipeline_name,
            pipeline_origin=pipeline_handle.get_origin(),
        )

        with external_pipeline_from_run(instance, run) as external_pipeline:
            assert external_pipeline.name == pipeline_handle.pipeline_name
