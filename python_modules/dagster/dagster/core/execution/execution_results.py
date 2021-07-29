from abc import abstractproperty
from typing import Any, Dict, List, Optional, cast

from dagster import check
from dagster.core.definitions import (
    GraphDefinition,
    Node,
    NodeHandle,
    PipelineDefinition,
    SolidDefinition,
)
from dagster.core.errors import DagsterInvariantViolationError
from dagster.core.events import DagsterEvent
from dagster.core.execution.build_resources import build_resources
from dagster.core.execution.context.input import InputContext
from dagster.core.execution.context.output import get_output_context
from dagster.core.execution.context_creation_pipeline import initialize_console_manager
from dagster.core.execution.plan.outputs import StepOutputHandle
from dagster.core.execution.plan.plan import ExecutionPlan
from dagster.core.execution.plan.step import StepKind
from dagster.core.execution.resources_init import get_dependencies, resolve_resource_dependencies
from dagster.core.instance import DagsterInstance
from dagster.core.system_config.objects import ResolvedRunConfig


def _filter_by_step_kind(event_list: List[DagsterEvent], kind: StepKind) -> List[DagsterEvent]:
    return [event for event in event_list if event.step_kind == kind]


def _filter_step_events_by_handle(
    event_list: List[DagsterEvent],
    ancestor_handle: Optional[NodeHandle],
    current_handle: NodeHandle,
) -> List[DagsterEvent]:
    events = []
    if ancestor_handle:
        handle_with_ancestor = cast(NodeHandle, current_handle.with_ancestor(ancestor_handle))
    else:
        handle_with_ancestor = current_handle

    for event in event_list:
        if event.is_step_event:
            solid_handle = cast(NodeHandle, event.solid_handle)
            if solid_handle.is_or_descends_from(handle_with_ancestor):
                events.append(event)

    return events


def _get_solid_handle_from_output(step_output_handle: StepOutputHandle) -> Optional[NodeHandle]:
    return NodeHandle.from_string(step_output_handle.step_key)


def _filter_outputs_by_handle(
    output_dict: Dict[StepOutputHandle, Any],
    ancestor_handle: Optional[NodeHandle],
    current_handle: NodeHandle,
):
    if ancestor_handle:
        handle_with_ancestor = current_handle.with_ancestor(ancestor_handle)
    else:
        handle_with_ancestor = current_handle

    outputs = {}
    for step_output_handle, value in output_dict.items():
        handle = _get_solid_handle_from_output(step_output_handle)
        if handle and handle_with_ancestor and handle.is_or_descends_from(handle_with_ancestor):
            outputs[step_output_handle] = value
    return outputs


class NodeExecutionResult:
    @property
    def success(self):
        """bool: Whether all steps in the execution were successful."""
        return all([not event.is_failure for event in self.event_list])

    @abstractproperty
    def output_values(self) -> Dict[str, Any]:
        raise NotImplementedError()

    @abstractproperty
    def event_list(self) -> List[DagsterEvent]:
        raise NotImplementedError()


class InProcessSolidResult(NodeExecutionResult):
    def __init__(
        self,
        solid_def: SolidDefinition,
        handle: NodeHandle,
        all_events: List[DagsterEvent],
        output_capture: Optional[Dict[StepOutputHandle, Any]],
        execution_plan: ExecutionPlan,
        pipeline_def: PipelineDefinition,
        run_config: Dict[str, Any],
        instance: DagsterInstance,
    ):
        self._solid_def = solid_def
        self._handle = handle
        self._event_list = all_events
        self._output_capture = output_capture
        self._execution_plan = execution_plan
        self._pipeline_def = pipeline_def
        self._run_config = run_config
        self._instance = instance

    @property
    def output_values(self) -> Dict[str, Any]:
        """
        The output values for the associated op/solid, keyed by output name.
        """
        solid_handle_as_str = str(
            self.handle
        )  # Handle takes into account aliasing and composition structure.

        results: Dict[str, Any] = {}

        if self._output_capture is not None:
            for step_output_handle, value in self._output_capture.items():
                if step_output_handle.step_key == solid_handle_as_str:
                    if step_output_handle.mapping_key:
                        if results.get(step_output_handle.output_name) is None:
                            results[step_output_handle.output_name] = {
                                step_output_handle.mapping_key: value
                            }
                        else:
                            results[step_output_handle.output_name][
                                step_output_handle.mapping_key
                            ] = value
                    else:
                        results[step_output_handle.output_name] = value
        elif self._pipeline_def.is_using_memoization:
            resource_defs = self._pipeline_def.get_mode_definition().resource_defs
            resource_deps = resolve_resource_dependencies(resource_defs)

            resource_defs_to_init = {}

            for output_def in self._solid_def.output_defs:
                resource_keys_to_init = get_dependencies(output_def.io_manager_key, resource_deps)
                for resource_key in resource_keys_to_init:
                    resource_defs_to_init[resource_key] = resource_defs[resource_key]

            resolved_run_config = ResolvedRunConfig.build(self._pipeline_def, self._run_config)
            all_resources_config = resolved_run_config.to_dict().get("resources", {})
            resource_config = {
                resource_key: config_val
                for resource_key, config_val in all_resources_config.items()
                if resource_key in resource_defs_to_init
            }

            log_manager = initialize_console_manager(None)

            with build_resources(
                resources=resource_defs_to_init,
                instance=self._instance,
                resource_config=resource_config,
                log_manager=log_manager,
            ) as resources:
                for output_def in self._solid_def.output_defs:
                    io_manager_key = output_def.io_manager_key
                    io_manager = getattr(resources, io_manager_key)
                    step_output_handle = StepOutputHandle(
                        step_key=solid_handle_as_str, output_name=output_def.name
                    )
                    version = self._execution_plan.get_version_for_step_output_handle(
                        step_output_handle
                    )

                    output_context = get_output_context(
                        execution_plan=self._execution_plan,
                        pipeline_def=self._pipeline_def,
                        resolved_run_config=resolved_run_config,
                        step_output_handle=step_output_handle,
                        run_id=None,
                        log_manager=log_manager,
                        step_context=None,
                        resources=resources,
                        version=version,
                    )

                    input_context = InputContext(
                        pipeline_name=self._pipeline_def.name,
                        solid_def=self._solid_def.name,
                        upstream_output=output_context,
                        resource_config=all_resources_config.get(io_manager_key),
                        resources=resources,
                        log_manager=log_manager,
                    )

                    obj = io_manager.load_input(input_context)

                    results[output_def.name] = obj

        return results

    @property
    def event_list(self) -> List[DagsterEvent]:
        return self._event_list

    @property
    def handle(self) -> NodeHandle:
        return self._handle


class InProcessGraphResult(NodeExecutionResult):
    def __init__(
        self,
        graph_def: GraphDefinition,
        handle: Optional[NodeHandle],
        all_events: List[DagsterEvent],
        output_capture: Optional[Dict[StepOutputHandle, Any]],
        execution_plan: ExecutionPlan,
        pipeline_def: PipelineDefinition,
        run_config: Dict[str, Any],
        instance: DagsterInstance,
    ):
        self._graph_def = graph_def
        self._handle = handle
        self._event_list = all_events
        self._output_capture = output_capture
        self._execution_plan = execution_plan
        self._pipeline_def = pipeline_def
        self._run_config = run_config
        self._instance = instance

    def _result_for_handle(self, solid: Node, handle: NodeHandle) -> NodeExecutionResult:
        node_def = solid.definition
        events_for_handle = _filter_step_events_by_handle(self.event_list, self.handle, handle)
        outputs_for_handle = (
            _filter_outputs_by_handle(self._output_capture, self.handle, handle)
            if self._output_capture
            else None
        )
        if self.handle:
            handle_with_ancestor = handle.with_ancestor(self.handle)
        else:
            handle_with_ancestor = handle

        if not handle_with_ancestor:
            raise DagsterInvariantViolationError(f"No handle provided for solid {solid.name}")

        if isinstance(node_def, SolidDefinition):
            return InProcessSolidResult(
                solid_def=node_def,
                handle=handle_with_ancestor,
                all_events=events_for_handle,
                output_capture=outputs_for_handle,
                execution_plan=self._execution_plan,
                pipeline_def=self._pipeline_def,
                run_config=self._run_config,
                instance=self._instance,
            )
        elif isinstance(node_def, GraphDefinition):
            return InProcessGraphResult(
                graph_def=node_def,
                handle=handle_with_ancestor,
                all_events=events_for_handle,
                output_capture=outputs_for_handle,
                execution_plan=self._execution_plan,
                pipeline_def=self._pipeline_def,
                run_config=self._run_config,
                instance=self._instance,
            )

        check.failed("Unhandled node type {node_def}")

    @property
    def output_values(self) -> Dict[str, Any]:
        """
        The values for any outputs that this associated graph maps.
        """
        values = {}

        for output_name in self._graph_def.output_dict:
            output_mapping = self._graph_def.get_output_mapping(output_name)

            inner_solid_values = self._result_for_handle(
                self._graph_def.solid_named(output_mapping.maps_from.solid_name),
                NodeHandle(output_mapping.maps_from.solid_name, None),
            ).output_values

            if inner_solid_values is not None:  # may be None if inner solid was skipped
                if output_mapping.maps_from.output_name in inner_solid_values:
                    values[output_name] = inner_solid_values[output_mapping.maps_from.output_name]

        return values

    @property
    def event_list(self) -> List[DagsterEvent]:
        return self._event_list

    @property
    def handle(self) -> Optional[NodeHandle]:
        return self._handle

    def result_for_node(self, name: str) -> NodeExecutionResult:
        """
        The inner result for a node within the graph.
        """

        if not self._graph_def.has_solid_named(name):
            raise DagsterInvariantViolationError(
                "Tried to get result for node '{name}' in '{container}'. No such top level "
                "node.".format(name=name, container=self._graph_def.name)
            )
        handle = NodeHandle(name, None)
        solid = self._graph_def.get_solid(handle)
        return self._result_for_handle(solid, handle)
