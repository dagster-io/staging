import pytest
from dagster.cli.workspace import Workspace
from dagster.utils.test.repo_from_module import basic_pipeline_from_module, basic_repo_from_module
from dagster.utils.test.repo_from_package import (
    basic_pipeline_from_package,
    basic_repo_from_package,
)

from .repo_from_python_file import basic_pipeline_from_file, basic_repo_from_file


def test_workspace_get():
    with Workspace.get() as workspace:
        assert len(workspace.repository_location_handles) == 3
        assert workspace.has_repository_location_handle("repo_from_file")
        assert workspace.has_repository_location_handle("repo_from_package")
        assert workspace.has_repository_location_handle("repo_from_module")


@pytest.mark.parametrize(
    "pipeline, repo, repo_location",
    [
        (basic_pipeline_from_file, basic_repo_from_file, "repo_from_file"),
        (basic_pipeline_from_module, basic_repo_from_module, "repo_from_module"),
        (basic_pipeline_from_package, basic_repo_from_package, "repo_from_package"),
    ],
)
def test_get_pipeline_from_source(pipeline, repo, repo_location):
    with Workspace.get() as workspace:
        with workspace.find_external_target(pipeline.name) as target:
            pipeline_def, external_pipeline, external_repository, repository_location = target
            assert pipeline_def.name == pipeline.name
            assert external_pipeline.handle.pipeline_name == pipeline.name
            assert external_pipeline.handle.repository_handle.repository_name == repo.name
            assert (
                external_pipeline.handle.repository_handle.repository_location_handle.location_name
                == repo_location
            )
            assert external_pipeline.handle.location_name == repo_location
            assert external_repository.name == repo.name
            assert (
                external_repository.handle.repository_location_handle.location_name == repo_location
            )
            assert repository_location.name == repo_location

        with workspace.reconstructable_from_target(pipeline.name) as target:
            assert target.pipeline_name == pipeline.name
            assert target.repository.get_definition().name == repo.name
