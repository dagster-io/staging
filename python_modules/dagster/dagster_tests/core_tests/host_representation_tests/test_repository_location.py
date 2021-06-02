import datetime
import sys

import mock
import pendulum
import pytest
from dagster import (
    PartitionSetDefinition,
    daily_schedule,
    file_relative_path,
    hourly_schedule,
    monthly_schedule,
    pipeline,
    repository,
    weekly_schedule,
)
from dagster.cli.workspace.dynamic_workspace import DynamicWorkspace
from dagster.core.host_representation.grpc_server_registry import ProcessGrpcServerRegistry
from dagster.core.host_representation.handle import RepositoryHandle
from dagster.core.host_representation.origin import ManagedGrpcPythonEnvRepositoryLocationOrigin
from dagster.core.host_representation.repository_location import (
    sync_get_external_partition_names_grpc,
)
from dagster.core.types.loadable_target_origin import LoadableTargetOrigin
from dagster.seven.compat.pendulum import create_pendulum_time
from dagster.utils.partitions import date_partition_range, identity_partition_selector


@pytest.fixture(name="no_grpc_partitions_requests", autouse=True)
def no_grpc_partitions_requests_fixture(monkeypatch):
    monkeypatch.delattr("dagster.api.snapshot_partition.sync_get_external_partition_names_grpc")


@pipeline
def noop_pipeline():
    pass


@hourly_schedule(pipeline_name="noop_pipeline", start_date=datetime.datetime(2021, 1, 1))
def hourly_noop_schedule():
    return {}


@daily_schedule(pipeline_name="noop_pipeline", start_date=datetime.datetime(2021, 1, 1))
def daily_noop_schedule():
    return {}


@weekly_schedule(pipeline_name="noop_pipeline", start_date=datetime.datetime(2021, 1, 1))
def weekly_noop_schedule():
    return {}


@monthly_schedule(pipeline_name="noop_pipeline", start_date=datetime.datetime(2021, 1, 1))
def monthly_noop_schedule():
    return {}


def noop_dynamic_schedule():
    # create weekly partition set
    schedule_name = "noop_dynamic_schedule"
    partition_set = PartitionSetDefinition(
        name="noop_dynamic_partition",
        pipeline_name=noop_pipeline.name,
        partition_fn=date_partition_range(
            start=datetime.datetime(2021, 1, 1), end=datetime.datetime(2021, 1, 4)
        ),
    )

    return partition_set.create_schedule_definition(
        schedule_name=schedule_name,
        cron_schedule="* * * * *",
        partition_selector=identity_partition_selector,
    )


@repository
def repo():
    return [
        noop_pipeline,
        hourly_noop_schedule,
        daily_noop_schedule,
        weekly_noop_schedule,
        monthly_noop_schedule,
        noop_dynamic_schedule(),
    ]


@pytest.fixture(name="workspace")
def dynamic_workspace_fixture():
    with ProcessGrpcServerRegistry(
        reload_interval=5, heartbeat_ttl=10
    ) as registry, DynamicWorkspace(registry) as workspace:
        yield workspace


@pytest.fixture(name="location")
def repository_location_fixture(workspace):
    origin = ManagedGrpcPythonEnvRepositoryLocationOrigin(
        loadable_target_origin=LoadableTargetOrigin(
            executable_path=sys.executable,
            attribute="repo",
            python_file=file_relative_path(__file__, "test_repository_location.py"),
        ),
    )

    yield workspace.get_location(origin)


@pytest.fixture(name="repo_handle")
def repository_handle_fixture(location):
    yield RepositoryHandle(repo.name, location)


@pytest.mark.parametrize(
    argnames=["schedule", "current_time", "expected_partitions"],
    ids=["hourly schedule", "daily schedule", "weekly schedule", "monthly schedule"],
    argvalues=[
        (
            hourly_noop_schedule,
            create_pendulum_time(2021, 1, 1, 3, 0),
            ["2021-01-01-00:00", "2021-01-01-01:00", "2021-01-01-02:00"],
        ),
        (
            daily_noop_schedule,
            create_pendulum_time(2021, 1, 4),
            ["2021-01-01", "2021-01-02", "2021-01-03"],
        ),
        (
            weekly_noop_schedule,
            create_pendulum_time(2021, 1, 31),
            ["2021-01-01", "2021-01-08", "2021-01-15", "2021-01-22"],
        ),
        (
            monthly_noop_schedule,
            create_pendulum_time(2021, 4, 1),
            ["2021-01", "2021-02", "2021-03"],
        ),
    ],
)
@mock.patch(
    "dagster.core.host_representation.repository_location.sync_get_external_partition_names_grpc"
)
def test_schedule_based_partition_params_external_partition_names(
    mock_sync_get_external_partition_names_grpc,
    schedule,
    current_time,
    expected_partitions,
    location,
    repo_handle,
):
    with pendulum.test(current_time):
        external_partition_names_data = location.get_external_partition_names(
            repo_handle, schedule.get_partition_set().name
        )

        assert external_partition_names_data.partition_names == expected_partitions
        assert not mock_sync_get_external_partition_names_grpc.called


@mock.patch(
    "dagster.core.host_representation.repository_location.sync_get_external_partition_names_grpc",
    wraps=sync_get_external_partition_names_grpc,
)
def test_dynamic_partition_params_external_partition_names(
    mock_sync_get_external_partition_names_grpc,
    location,
    repo_handle,
):
    external_partition_names_data = location.get_external_partition_names(
        repo_handle, "noop_dynamic_partition"
    )

    assert external_partition_names_data.partition_names == [
        "2021-01-01",
        "2021-01-02",
        "2021-01-03",
    ]

    assert mock_sync_get_external_partition_names_grpc.called
