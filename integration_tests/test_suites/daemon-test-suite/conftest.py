import sys
from contextlib import contextmanager

import pytest
from dagster import file_relative_path
from dagster.core.host_representation import (
    ManagedGrpcPythonEnvRepositoryLocationOrigin,
    PipelineHandle,
)
from dagster.core.types.loadable_target_origin import LoadableTargetOrigin


@contextmanager
def get_example_repository_location():
    loadable_target_origin = LoadableTargetOrigin(
        executable_path=sys.executable,
        python_file=file_relative_path(__file__, "repo.py"),
    )
    location_name = "example_repo_location"

    origin = ManagedGrpcPythonEnvRepositoryLocationOrigin(loadable_target_origin, location_name)

    with origin.create_test_location() as location:
        yield location


@pytest.fixture
def foo_example_workspace():
    with Workspace(
        PythonFileTarget(
            python_file=file_relative_path(__file__, "repo.py"),
            attribute=None,
            working_directory=None,
            location_name="example_repo_location",
        )
    ) as workspace:
        yield workspace


@pytest.fixture
def foo_example_repo(foo_example_workspace):
    return foo_example_workspace.get_repository_location("example_repo_location").get_repository(
        "example_repo"
    )


@pytest.fixture
def foo_pipeline_handle(foo_example_repo):  # pylint: disable=redefined-outer-name
    return PipelineHandle("foo_pipeline", foo_example_repo.handle)
