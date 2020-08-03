from dagster import seven
from dagster.api.launch_partition_backfill import (
    sync_launch_partition_backfill,
    sync_launch_partition_backfill_grpc,
)
from dagster.core.host_representation import (
    ExternalPartitionBackfillData,
    ExternalPartitionExecutionErrorData,
)
from dagster.core.instance import DagsterInstance
from dagster.core.storage.pipeline_run import PipelineRun, PipelineRunsFilter

from .utils import get_bar_grpc_repo_handle, get_bar_repo_handle


def test_external_partition_backfill():
    with seven.TemporaryDirectory() as temp_dir:
        instance = DagsterInstance.local_temp(temp_dir)
        repository_handle = get_bar_repo_handle()
        data = sync_launch_partition_backfill(
            instance, repository_handle, 'baz_partitions', ['a', 'b', 'c']
        )
        assert isinstance(data, ExternalPartitionBackfillData)
        backfill_filter = PipelineRunsFilter(
            tags=PipelineRun.tags_for_backfill_id(data.backfill_id)
        )
        runs = instance.get_runs(filters=backfill_filter)
        assert len(runs) == 3


def test_external_partition_backfill_grpc():
    with seven.TemporaryDirectory() as temp_dir:
        instance = DagsterInstance.local_temp(temp_dir)
        repository_handle = get_bar_grpc_repo_handle()
        data = sync_launch_partition_backfill_grpc(
            repository_handle.repository_location_handle.client,
            instance,
            repository_handle,
            'baz_partitions',
            ['a', 'b', 'c'],
        )
        assert isinstance(data, ExternalPartitionBackfillData)
        backfill_filter = PipelineRunsFilter(
            tags=PipelineRun.tags_for_backfill_id(data.backfill_id)
        )
        runs = instance.get_runs(filters=backfill_filter)
        assert len(runs) == 3


def test_external_partition_backfill_error():
    with seven.TemporaryDirectory() as temp_dir:
        instance = DagsterInstance.local_temp(temp_dir)
        repository_handle = get_bar_repo_handle()
        error = sync_launch_partition_backfill(
            instance, repository_handle, 'error_partitions', ['a', 'b', 'c']
        )
        assert isinstance(error, ExternalPartitionExecutionErrorData)
        assert 'womp womp' in error.error.to_string()
