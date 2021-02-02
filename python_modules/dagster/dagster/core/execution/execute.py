from typing import Dict, Optional

from dagster import check
from dagster.core.definitions import (
    LoggerDefinition,
    ModeDefinition,
    NodeDefinition,
    PipelineDefinition,
    ResourceDefinition,
)
from dagster.core.definitions.dependency import SolidHandle
from dagster.core.definitions.pipeline_base import InMemoryPipeline
from dagster.core.instance import DagsterInstance
from dagster.core.storage.mem_io_manager import mem_io_manager
from dagster.utils import merge_dicts

from .api import (
    ExecuteRunWithPlanIterable,
    create_execution_plan,
    ephemeral_instance_if_missing,
    pipeline_execution_iterator,
)
from .context_creation_pipeline import PipelineExecutionContextManager
from .results import ExecutionResult


def execute_node(
    node: NodeDefinition,
    run_config: Optional[dict] = None,
    resource_defs: Optional[Dict[str, ResourceDefinition]] = None,
    logger_defs: Optional[Dict[str, LoggerDefinition]] = None,
    instance: DagsterInstance = None,
) -> ExecutionResult:
    node = check.inst_param(node, "node", NodeDefinition)
    resource_defs = check.opt_dict_param(
        resource_defs, "resource_defs", key_type=str, value_type=ResourceDefinition
    )
    run_config = check.opt_dict_param(run_config, "run_config", key_type=str)

    node_defs = [node]

    mode_def = ModeDefinition(
        "created",
        resource_defs=merge_dicts(resource_defs, {"DAGSTER_SYSTEM_IO_MANAGER": mem_io_manager}),
        logger_defs=logger_defs,
    )

    pipeline_def = PipelineDefinition(
        node_defs, name=f"ephemeral_{node.name}_node_pipeline", mode_defs=[mode_def],
    )

    pipeline = InMemoryPipeline(pipeline_def)

    execution_plan = create_execution_plan(pipeline, run_config=run_config, mode=mode_def.name)

    with ephemeral_instance_if_missing(instance) as execute_instance:
        pipeline_run = execute_instance.create_run_for_pipeline(
            pipeline_def=pipeline_def, run_config=run_config, mode=mode_def.name,
        )

        _execute_run_iterable = ExecuteRunWithPlanIterable(
            execution_plan=execution_plan,
            iterator=pipeline_execution_iterator,
            execution_context_manager=PipelineExecutionContextManager(
                execution_plan=execution_plan,
                pipeline_run=pipeline_run,
                instance=execute_instance,
                run_config=run_config,
            ),
        )
        event_list = list(_execute_run_iterable)

    top_level_node_handle = SolidHandle.from_string(node.name)

    event_list_for_top_lvl_node = [
        event
        for event in event_list
        if event.solid_handle and event.solid_handle.is_or_descends_from(top_level_node_handle)
    ]

    return ExecutionResult(node, event_list_for_top_lvl_node)
