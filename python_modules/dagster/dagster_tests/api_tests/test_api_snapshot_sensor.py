from dagster import seven
from dagster.api.snapshot_sensor import (
    sync_get_external_sensor_execution_data,
    sync_get_external_sensor_execution_data_ephemeral_grpc,
)
from dagster.core.host_representation.external_data import (
    ExternalSensorExecutionData,
    ExternalSensorExecutionErrorData,
)
from dagster.core.instance import DagsterInstance

from .utils import get_bar_repo_handle


def test_external_sensor():
    repository_handle = get_bar_repo_handle()
    with seven.TemporaryDirectory() as temp_dir:
        instance = DagsterInstance.local_temp(temp_dir)
        result = sync_get_external_sensor_execution_data(instance, repository_handle, "sensor_foo")
        assert isinstance(result, ExternalSensorExecutionData)
        assert result.job_data.run_config == {"foo": "FOO"}


def test_external_sensor_error():
    repository_handle = get_bar_repo_handle()
    with seven.TemporaryDirectory() as temp_dir:
        instance = DagsterInstance.local_temp(temp_dir)
        result = sync_get_external_sensor_execution_data(
            instance, repository_handle, "sensor_error"
        )
        assert isinstance(result, ExternalSensorExecutionErrorData)
        assert "womp womp" in result.error.to_string()


def test_external_sensor_grpc():
    repository_handle = get_bar_repo_handle()
    with seven.TemporaryDirectory() as temp_dir:
        instance = DagsterInstance.local_temp(temp_dir)
        result = sync_get_external_sensor_execution_data_ephemeral_grpc(
            instance, repository_handle, "sensor_foo"
        )
        assert isinstance(result, ExternalSensorExecutionData)
        assert result.job_data.run_config == {"foo": "FOO"}
