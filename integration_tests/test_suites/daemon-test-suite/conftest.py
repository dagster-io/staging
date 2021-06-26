import sys
from contextlib import contextmanager

import pytest
from dagster import file_relative_path
from dagster.core.host_representation import (
    ManagedGrpcPythonEnvRepositoryLocationOrigin,
    PipelineHandle,
)
from dagster.core.types.loadable_target_origin import LoadableTargetOrigin
from dagster.core.test_utils import instance_for_test
from dagster.core.workspace import WorkspaceProcessContext


@pytest.fixture(name="instance")
def instance_fixture():
    with instance_for_test(
        overrides={
            "run_coordinator": {
                "module": "dagster.core.run_coordinator",
                "class": "QueuedRunCoordinator",
                "config": {"dequeue_interval_seconds": 1},
            }
        }
    ) as instance:
        yield instance


@pytest.fixture
def foo_example_workspace(instance):
    with WorkspaceProcessContext(
        instance,
        PythonFileTarget(
            python_file=file_relative_path(__file__, "repo.py"),
            attribute=None,
            working_directory=None,
            location_name="example_repo_location",
        ),
    ) as workspace_process_context:
        yield workspace.create_request_context()


@pytest.fixture
def foo_example_repo(foo_example_workspace):
    return foo_example_workspace.get_repository_location("example_repo_location").get_repository(
        "example_repo"
    )
