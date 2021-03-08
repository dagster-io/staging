from collections import defaultdict
from typing import Any, Dict, Optional, Union

from dagster import check
from dagster.core.definitions import (
    DependencyDefinition,
    LoggerDefinition,
    ModeDefinition,
    NodeDefinition,
    OutputDefinition,
    PipelineDefinition,
    ResourceDefinition,
    SolidDefinition,
)
from dagster.core.definitions.decorators.solid import solid
from dagster.core.definitions.dependency import SolidHandle
from dagster.core.definitions.pipeline_base import InMemoryPipeline
from dagster.core.execution.plan.outputs import StepOutputHandle
from dagster.core.instance import DagsterInstance
from dagster.core.storage.io_manager import IOManager, IOManagerDefinition
from dagster.core.storage.mem_io_manager import mem_io_manager
from dagster.utils import merge_dicts
from dagster.utils.merger import deep_merge_dicts

from .api import (
    ExecuteRunWithPlanIterable,
    create_execution_plan,
    ephemeral_instance_if_missing,
    pipeline_execution_iterator,
)
from .context_creation_pipeline import PipelineExecutionContextManager
from .execution_results import InProcessGraphResult, InProcessSolidResult, NodeExecutionResult

EPHEMERAL_IO_MANAGER_KEY = "system__execute_solid_ephemeral_node_io_manager"


def _create_value_solid(input_name, input_value):
    @solid(name=input_name, output_defs=[OutputDefinition(io_manager_key=EPHEMERAL_IO_MANAGER_KEY)])
    def input_solid(_):
        return input_value

    return input_solid


def _resolve_config_if_exists(field_name, node_name, config):
    return {"solids": {node_name: {field_name: config}}} if config else {}


def _deep_merge_multiple_dicts(*args):
    result = {}
    for arg in args:
        result = deep_merge_dicts(result, arg)
    return result


def execute_in_process(
    node: NodeDefinition,
    solid_config: Optional[dict] = None,
    composed_config: Optional[dict] = None,
    resources: Optional[Dict[str, Union[Any, ResourceDefinition]]] = None,
    loggers: Optional[Dict[str, LoggerDefinition]] = None,
    input_values: Optional[Dict[str, Any]] = None,
    output_config: Optional[Dict[str, Any]] = None,
    input_config: Optional[Dict[str, Any]] = None,
    resource_config: Optional[Dict[str, Any]] = None,
    instance: DagsterInstance = None,
    output_capturing_enabled: Optional[bool] = True,
) -> NodeExecutionResult:
    node = check.inst_param(node, "node", NodeDefinition)
    solid_config = check.opt_dict_param(solid_config, "solid_config", key_type=str)
    composed_config = check.opt_dict_param(composed_config, "composed_config", key_type=str)
    resources = check.opt_dict_param(resources, "resources", key_type=str)
    loggers = check.opt_dict_param(loggers, "logger", key_type=str, value_type=LoggerDefinition)
    input_values = check.opt_dict_param(input_values, "input_values", key_type=str)
    output_config = check.opt_dict_param(output_config, "output_config", key_type=str)
    input_config = check.opt_dict_param(input_config, "input_config", key_type=str)
    resource_config = check.opt_dict_param(resource_config, "resource_config", key_type=str)
    instance = check.opt_inst_param(instance, "instance", DagsterInstance)
    output_capturing_enabled = check.bool_param(
        output_capturing_enabled, "output_capturing_enabled"
    )

    if isinstance(node, SolidDefinition):
        check.invariant(
            not composed_config,
            "The `composed_config` argument should only be provided when executing graphs "
            "whose internal solids require config.",
        )
    composed_config = check.opt_dict_param(composed_config, "composed_config", key_type=str)
    input_values = check.opt_dict_param(input_values, "input_values", key_type=str)

    node_defs = [node]

    dependencies: Dict[str, Dict[str, DependencyDefinition]] = defaultdict(dict)

    for input_name, input_value in input_values.items():
        dependencies[node.name][input_name] = DependencyDefinition(input_name)
        node_defs.append(_create_value_solid(input_name, input_value))

    resource_defs = {}
    for key, val in resources.items():
        if isinstance(val, ResourceDefinition):
            resource_defs[key] = val
        elif isinstance(val, IOManager):
            resource_defs[key] = IOManagerDefinition.hardcoded_io_manager(val)
        else:
            resource_defs[key] = ResourceDefinition.hardcoded_resource(val)

    run_config = _deep_merge_multiple_dicts(
        _resolve_config_if_exists("config", node.name, solid_config),
        _resolve_config_if_exists("solids", node.name, composed_config),
        _resolve_config_if_exists("outputs", node.name, output_config),
        _resolve_config_if_exists("inputs", node.name, input_config),
    )
    # resource config has a different format, so handle this separately.
    resolved_resource_config = {
        "resources": {
            resource_key: {"config": config_values}
            for resource_key, config_values in resource_config.items()
        }
    }
    run_config = deep_merge_dicts(run_config, resolved_resource_config)

    mode_def = ModeDefinition(
        "created",
        resource_defs=merge_dicts(resource_defs, {EPHEMERAL_IO_MANAGER_KEY: mem_io_manager}),
        logger_defs=loggers,
    )

    pipeline_def = PipelineDefinition(
        node_defs,
        name=f"ephemeral_{node.name}_node_pipeline",
        mode_defs=[mode_def],
        dependencies=dependencies,
    )

    pipeline = InMemoryPipeline(pipeline_def)

    execution_plan = create_execution_plan(pipeline, run_config=run_config, mode=mode_def.name)

    recorder: Dict[StepOutputHandle, Any] = {}

    with ephemeral_instance_if_missing(instance) as execute_instance:
        pipeline_run = execute_instance.create_run_for_pipeline(
            pipeline_def=pipeline_def,
            run_config=run_config,
            mode=mode_def.name,
        )

        _execute_run_iterable = ExecuteRunWithPlanIterable(
            execution_plan=execution_plan,
            iterator=pipeline_execution_iterator,
            execution_context_manager=PipelineExecutionContextManager(
                execution_plan=execution_plan,
                pipeline_run=pipeline_run,
                instance=execute_instance,
                run_config=run_config,
                output_capture=recorder if output_capturing_enabled else None,
            ),
        )
        event_list = list(_execute_run_iterable)

    top_level_node_handle = SolidHandle.from_string(node.name)

    event_list_for_top_lvl_node = [
        event
        for event in event_list
        if event.solid_handle and event.solid_handle.is_or_descends_from(top_level_node_handle)
    ]

    if isinstance(node, SolidDefinition):
        return InProcessSolidResult(
            node, SolidHandle(node.name, None), event_list_for_top_lvl_node, recorder
        )
    else:
        return InProcessGraphResult(
            node, SolidHandle(node.name, None), event_list_for_top_lvl_node, recorder
        )
