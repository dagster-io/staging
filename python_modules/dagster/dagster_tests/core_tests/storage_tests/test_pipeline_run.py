import pytest

from dagster import check
from dagster.core.origin import PipelineOrigin
from dagster.core.storage.pipeline_run import PipelineRun, PipelineRunStatus


class FakePipelineOrigin(PipelineOrigin):
    def get_repo_cli_args(self):
        pass

    @property
    def executable_path(self):
        pass


def test_pipeline_origin_check():

    PipelineRun(status=PipelineRunStatus.QUEUED, pipeline_origin=FakePipelineOrigin())

    with pytest.raises(check.CheckError):
        PipelineRun(status=PipelineRunStatus.QUEUED)

    with pytest.raises(check.CheckError):
        PipelineRun().with_status(PipelineRunStatus.QUEUED)
