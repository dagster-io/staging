from dagster_tests.api_tests.utils import get_foo_pipeline_handle

from dagster.core.storage.pipeline_run import PipelineRun
from dagster.utils import merge_dicts


def new_pipeline_run(**kwargs):  # pylint: disable=redefined-outer-name
    """
    Returns a pipeline run without storing it in the run storage
    """
    pipeline_handle = get_foo_pipeline_handle()
    pipeline_args = merge_dicts(
        {"pipeline_name": "foo", "pipeline_origin": pipeline_handle.get_origin(),}, kwargs,
    )
    return PipelineRun(**pipeline_args)
