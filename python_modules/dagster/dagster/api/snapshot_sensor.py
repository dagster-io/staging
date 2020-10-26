from dagster import check
from dagster.core.host_representation.external_data import (
    ExternalSensorExecutionData,
    ExternalSensorExecutionErrorData,
)
from dagster.core.host_representation.handle import RepositoryHandle
from dagster.core.types.loadable_target_origin import LoadableTargetOrigin
from dagster.grpc.types import SensorExecutionArgs

from .utils import execute_unary_api_cli_command


def sync_get_external_sensor_execution_data(instance, repository_handle, sensor_name):

    check.inst_param(repository_handle, "repository_handle", RepositoryHandle)
    check.str_param(sensor_name, "sensor_name")
    origin = repository_handle.get_origin()

    return check.inst(
        execute_unary_api_cli_command(
            origin.executable_path,
            "sensor_config",
            SensorExecutionArgs(
                repository_origin=origin, instance_ref=instance.get_ref(), sensor_name=sensor_name,
            ),
        ),
        (ExternalSensorExecutionData, ExternalSensorExecutionErrorData),
    )


def sync_get_external_sensor_execution_data_ephemeral_grpc(
    instance, repository_handle, sensor_name
):
    from dagster.grpc.client import ephemeral_grpc_api_client

    origin = repository_handle.get_origin()
    with ephemeral_grpc_api_client(
        LoadableTargetOrigin(executable_path=origin.executable_path)
    ) as api_client:
        return sync_get_external_sensor_execution_data_grpc(
            api_client, instance, repository_handle, sensor_name
        )


def sync_get_external_sensor_execution_data_grpc(
    api_client, instance, repository_handle, sensor_name
):
    check.inst_param(repository_handle, "repository_handle", RepositoryHandle)
    check.str_param(sensor_name, "sensor_name")

    origin = repository_handle.get_origin()

    return check.inst(
        api_client.external_sensor_execution(
            sensor_execution_args=SensorExecutionArgs(
                repository_origin=origin, instance_ref=instance.get_ref(), sensor_name=sensor_name,
            )
        ),
        (ExternalSensorExecutionData, ExternalSensorExecutionErrorData),
    )
