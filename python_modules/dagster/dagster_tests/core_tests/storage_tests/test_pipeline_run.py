import sys

import pytest
from dagster import check
from dagster.core.code_pointer import ModuleCodePointer
from dagster.core.origin import PipelinePythonOrigin, RepositoryPythonOrigin
from dagster.core.storage.pipeline_run import PipelineRun, PipelineRunStatus


def test_queued_pipeline_origin_check():

    fake_pipeline_origin = PipelinePythonOrigin(
        "fake_pipeline", RepositoryPythonOrigin(sys.executable, ModuleCodePointer("fake", "fake"))
    )

    PipelineRun(status=PipelineRunStatus.QUEUED, pipeline_origin=fake_pipeline_origin)

    with pytest.raises(check.CheckError):
        PipelineRun(status=PipelineRunStatus.QUEUED)

    with pytest.raises(check.CheckError):
        PipelineRun().with_status(PipelineRunStatus.QUEUED)
