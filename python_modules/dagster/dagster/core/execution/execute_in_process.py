from collections import defaultdict, namedtuple
from typing import Any, Dict, Optional, Union

from dagster import check
from dagster.core.definitions import (
    DependencyDefinition,
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
from dagster.core.definitions.resource import Resources
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


class FromInputConfig(namedtuple("_FromInputConfig", "input_dict")):
    def __new__(cls, input_config_dict):
        input_config_dict = check.dict_param(input_config_dict, "input_config_dict", key_type=str)
        return super(FromInputConfig, cls).__new__(
            cls,
            input_dict=input_config_dict,
        )


def _create_value_solid(input_name, input_value):
    @solid(name=input_name, output_defs=[OutputDefinition(io_manager_key=EPHEMERAL_IO_MANAGER_KEY)])
    def input_solid(_):
        return input_value

    return input_solid


def _get_resources_as_dict(resources):
    if isinstance(resources, dict):
        return resources
    elif isinstance(resources, Resources):
        return {key: getattr(resources, key) for key in resources._asdict()}
    elif resources is None:
        return {}


def _resolve_config_if_exists(node_name, config):
    return {"solids": {node_name: config}} if config else {}


def _deep_merge_multiple_dicts(*args):
    result = {}
    for arg in args:
        result = deep_merge_dicts(result, arg)
    return result


def execute_in_process(
    node: NodeDefinition,
    config: Optional[dict] = None,
    resources: Optional[Union[Dict[str, Union[Any, ResourceDefinition]], Resources]] = None,
    input_values: Optional[Union[Dict[str, Any], FromInputConfig]] = None,
    instance: DagsterInstance = None,
) -> NodeExecutionResult:
    node = check.inst_param(node, "node", NodeDefinition)
    config = check.opt_dict_param(config, "config", key_type=str)
    resources = check.opt_inst_param(resources, "resources", (dict, Resources))
    input_values = check.opt_inst_param(input_values, "input_values", (dict, FromInputConfig))
    instance = check.opt_inst_param(instance, "instance", DagsterInstance)

    node_defs = [node]
    dependencies: Dict[str, Dict[str, DependencyDefinition]] = defaultdict(dict)

    if isinstance(input_values, dict):
        for input_name, input_value in input_values.items():
            dependencies[node.name][input_name] = DependencyDefinition(input_name)
            node_defs.append(_create_value_solid(input_name, input_value))

    resource_defs = {}
    for key, val in _get_resources_as_dict(resources).items():
        if isinstance(val, ResourceDefinition):
            resource_defs[key] = val
        elif isinstance(val, IOManager):
            resource_defs[key] = IOManagerDefinition.hardcoded_io_manager(val)
        else:
            resource_defs[key] = ResourceDefinition.hardcoded_resource(val)

    input_config = _resolve_config_if_exists(
        node.name,
        {"inputs": input_values.input_dict} if isinstance(input_values, FromInputConfig) else None,
    )
    if config:
        node_config = config if list(config.keys()) == ["solids"] else {"config": config}
    else:
        node_config = None
    run_config = _deep_merge_multiple_dicts(
        _resolve_config_if_exists(node.name, node_config),
        input_config,
    )

    mode_def = ModeDefinition(
        "created",
        resource_defs=merge_dicts(resource_defs, {EPHEMERAL_IO_MANAGER_KEY: mem_io_manager}),
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
                output_capture=recorder,
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
