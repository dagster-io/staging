from __future__ import absolute_import

from graphql.execution.base import ResolveInfo

from dagster import check
from dagster.core.host_representation import RepositorySelector
from dagster.core.host_representation.external_data import (
    ExternalPartitionBackfillData,
    ExternalPartitionExecutionErrorData,
)
from dagster.core.host_representation.selector import PipelineSelector

from ..external import get_external_pipeline_or_raise
from ..utils import ExecutionMetadata, ExecutionParams, capture_dauphin_error
from .run_lifecycle import create_valid_pipeline_run


@capture_dauphin_error
def launch_pipeline_reexecution(graphene_info, execution_params):
    return _launch_pipeline_execution(graphene_info, execution_params, is_reexecuted=True)


@capture_dauphin_error
def launch_pipeline_execution(graphene_info, execution_params):
    return _launch_pipeline_execution(graphene_info, execution_params)


@capture_dauphin_error
def create_and_launch_partition_backfill(graphene_info, backfill_params):
    partition_set_selector = backfill_params.get('selector')
    partition_set_name = partition_set_selector.get('partitionSetName')
    repository_selector = RepositorySelector.from_graphql_input(
        partition_set_selector.get('repositorySelector')
    )
    location = graphene_info.context.get_repository_location(repository_selector.location_name)
    repository = location.get_repository(repository_selector.repository_name)
    matches = [
        partition_set
        for partition_set in repository.get_external_partition_sets()
        if partition_set.name == partition_set_selector.get('partitionSetName')
    ]
    if not matches:
        return graphene_info.schema.type_named('PartitionSetNotFoundError')(partition_set_name)

    external_partition_set = next(iter(matches))
    external_pipeline = repository.get_full_external_pipeline(external_partition_set.pipeline_name)
    pipeline_selector = PipelineSelector(
        location_name=location.name,
        repository_name=repository.name,
        pipeline_name=external_pipeline.name,
        solid_selection=external_partition_set.solid_selection,
    )

    partition_names = backfill_params.get('partitionNames')

    result = graphene_info.context.get_external_partition_backfill_data(
        repository.handle, partition_set_name, partition_names
    )

    if isinstance(result, ExternalPartitionExecutionErrorData):
        return graphene_info.schema.type_named('PythonError')(result.error)

    assert isinstance(result, ExternalPartitionBackfillData)

    for run_data in result.run_data:
        execution_params = ExecutionParams(
            selector=pipeline_selector,
            run_config=run_data.run_config,
            mode=external_partition_set.mode,
            execution_metadata=ExecutionMetadata(run_id=None, tags=run_data.tags),
            step_keys=None,
        )
        pipeline_run = create_valid_pipeline_run(graphene_info, external_pipeline, execution_params)
        graphene_info.context.instance.launch_run(pipeline_run.run_id, external_pipeline)

    return graphene_info.schema.type_named('PartitionBackfillSuccess')(
        backfill_id=result.backfill_id
    )


def do_launch(graphene_info, execution_params, is_reexecuted=False):
    check.inst_param(graphene_info, 'graphene_info', ResolveInfo)
    check.inst_param(execution_params, 'execution_params', ExecutionParams)
    check.bool_param(is_reexecuted, 'is_reexecuted')

    if is_reexecuted:
        # required fields for re-execution
        execution_metadata = check.inst_param(
            execution_params.execution_metadata, 'execution_metadata', ExecutionMetadata
        )
        check.str_param(execution_metadata.root_run_id, 'root_run_id')
        check.str_param(execution_metadata.parent_run_id, 'parent_run_id')

    external_pipeline = get_external_pipeline_or_raise(graphene_info, execution_params.selector)

    pipeline_run = create_valid_pipeline_run(graphene_info, external_pipeline, execution_params)

    return graphene_info.context.instance.launch_run(
        pipeline_run.run_id, external_pipeline=external_pipeline
    )


def _launch_pipeline_execution(graphene_info, execution_params, is_reexecuted=False):
    check.inst_param(graphene_info, 'graphene_info', ResolveInfo)
    check.inst_param(execution_params, 'execution_params', ExecutionParams)
    check.bool_param(is_reexecuted, 'is_reexecuted')

    run = do_launch(graphene_info, execution_params, is_reexecuted)

    return graphene_info.schema.type_named('LaunchPipelineRunSuccess')(
        run=graphene_info.schema.type_named('PipelineRun')(run)
    )
