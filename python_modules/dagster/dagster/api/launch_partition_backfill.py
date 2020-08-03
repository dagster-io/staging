from dagster import check
from dagster.core.host_representation.external_data import (
    ExternalPartitionBackfillData,
    ExternalPartitionExecutionErrorData,
)
from dagster.core.host_representation.handle import RepositoryHandle
from dagster.core.instance import DagsterInstance
from dagster.grpc.types import PartitionBackfillArgs

from .utils import execute_unary_api_cli_command


def sync_launch_partition_backfill(
    instance, repository_handle, partition_set_name, partition_names
):
    check.inst_param(instance, 'instance', DagsterInstance)
    check.inst_param(repository_handle, 'repository_handle', RepositoryHandle)
    check.str_param(partition_set_name, 'partition_set_name')
    check.list_param(partition_names, 'partition_names', of_type=str)

    repository_origin = repository_handle.get_origin()

    return check.inst(
        execute_unary_api_cli_command(
            repository_origin.executable_path,
            'partition_backfill',
            PartitionBackfillArgs(
                instance_ref=instance.get_ref(),
                repository_origin=repository_origin,
                partition_set_name=partition_set_name,
                partition_names=partition_names,
            ),
        ),
        (ExternalPartitionBackfillData, ExternalPartitionExecutionErrorData),
    )


def sync_launch_partition_backfill_grpc(
    api_client, instance, repository_handle, partition_set_name, partition_names
):
    from dagster.grpc.client import DagsterGrpcClient

    check.inst_param(api_client, 'api_client', DagsterGrpcClient)
    check.inst_param(instance, 'instance', DagsterInstance)
    check.inst_param(repository_handle, 'repository_handle', RepositoryHandle)
    check.str_param(partition_set_name, 'partition_set_name')
    check.list_param(partition_names, 'partition_names', of_type=str)

    repository_origin = repository_handle.get_origin()

    return check.inst(
        api_client.external_partition_backfill(
            partition_backfill_args=PartitionBackfillArgs(
                instance_ref=instance.get_ref(),
                repository_origin=repository_origin,
                partition_set_name=partition_set_name,
                partition_names=partition_names,
            ),
        ),
        (ExternalPartitionBackfillData, ExternalPartitionExecutionErrorData),
    )
