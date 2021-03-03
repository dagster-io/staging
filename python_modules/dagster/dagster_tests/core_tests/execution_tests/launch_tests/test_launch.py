from dagster.core.execution.launch import launch

from .test_workspace.repo.repo import basic_pipeline


def test_launch_from_pipeline_def():
    launch(basic_pipeline)
