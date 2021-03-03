from abc import abstractproperty
from typing import Any, Dict, List, Optional

from dagster import DagsterEvent, check
from dagster.core.definitions import (
    GraphDefinition,
    PipelineDefinition,
    ResourceDefinition,
    Solid,
    SolidDefinition,
    SolidHandle,
)
from dagster.core.errors import DagsterInvariantViolationError
from dagster.core.execution.build_resources import initialize_console_manager
from dagster.core.execution.context.system import InputContext, OutputContext
from dagster.core.execution.plan.handle import ResolvedFromDynamicStepHandle, StepHandle
from dagster.core.execution.plan.outputs import StepOutputHandle
from dagster.core.execution.plan.step import StepKind
from dagster.core.execution.resources_init import ScopedResourcesBuilder
from dagster.core.host_representation import ExternalExecutionPlan
from dagster.core.storage.io_manager import IOManager
from dagster.core.storage.pipeline_run import PipelineRun
from dagster.core.system_config.objects import EnvironmentConfig


def _filter_by_step_kind(event_list: List[DagsterEvent], kind: StepKind) -> List[DagsterEvent]:
    return [event for event in event_list if event.step_kind == kind]


def _filter_step_events_by_handle(
    event_list: List[DagsterEvent],
    ancestor_handle: Optional[SolidHandle],
    current_handle: SolidHandle,
) -> List[DagsterEvent]:
    events = []
    handle_with_ancestor = current_handle.with_ancestor(ancestor_handle)
    for event in event_list:
        if event.is_step_event:
            if event.solid_handle.is_or_descends_from(handle_with_ancestor):
                events.append(event)

    return events


def _get_solid_handle_from_output(step_output_handle: StepOutputHandle) -> Optional[SolidHandle]:
    return SolidHandle.from_string(step_output_handle.step_key)


def _filter_outputs_by_handle(
    output_dict: Dict[StepOutputHandle, Any],
    ancestor_handle: SolidHandle,
    current_handle: SolidHandle,
):
    handle_with_ancestor = current_handle.with_ancestor(ancestor_handle)
    outputs = {}
    for step_output_handle, value in output_dict.items():
        handle = _get_solid_handle_from_output(step_output_handle)
        if handle and handle_with_ancestor and handle.is_or_descends_from(handle_with_ancestor):
            outputs[step_output_handle] = value
    return outputs


def _resolve_step_events_by_kind(
    event_list: List[DagsterEvent],
) -> Dict[StepKind, List[DagsterEvent]]:
    step_events_by_kind: Dict[StepKind, List[DagsterEvent]] = {}
    for event in event_list:
        if not event.is_step_event:
            continue
        if event.step_kind not in step_events_by_kind:
            step_events_by_kind[event.step_kind] = []
        step_events_by_kind[event.step_kind].append(event)
    return step_events_by_kind


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

    @property
    def handle(self) -> SolidHandle:
        raise NotImplementedError()


class SubmittedPipelineResult:
    def __init__(
        self,
        pipeline_def: PipelineDefinition,
        all_events: List[DagsterEvent],
        resources: ScopedResourcesBuilder,
        resource_defs: Dict[str, ResourceDefinition],
        mode: str,
        run_config: Optional[Dict[str, Any]],
        pipeline_run: PipelineRun,
        external_execution_plan: ExternalExecutionPlan,
    ):
        self._pipeline_def = pipeline_def
        self._event_list = all_events
        self._resources = resources
        self._resource_defs = resource_defs
        self._run_config = run_config
        self._pipeline_run = pipeline_run
        self._external_execution_plan = external_execution_plan
        self._environment_config = EnvironmentConfig.build(
            self._pipeline_def, run_config=run_config, mode=mode
        )

    @property
    def event_list(self) -> List[DagsterEvent]:
        return self._event_list

    @property
    def success(self):
        """bool: Whether all steps in the execution were successful."""
        return all([not event.is_failure for event in self.event_list])

    def result_for_node(self, name: str) -> NodeExecutionResult:
        if not self._pipeline_def.has_solid_named(name):
            raise DagsterInvariantViolationError(
                "Tried to get result for node '{name}' in '{container}'. No such top level "
                "node.".format(name=name, container=self._pipeline_def.name)
            )
        handle = SolidHandle(name, None)
        solid = self._pipeline_def.get_solid(handle)
        return self._result_for_handle(solid, handle)

    def _result_for_handle(self, solid: Solid, handle: SolidHandle) -> NodeExecutionResult:
        node_def = solid.definition
        events_for_handle = _filter_step_events_by_handle(self.event_list, None, handle)
        if not handle:
            raise DagsterInvariantViolationError(f"No handle provided for solid {solid.name}")
        return ExternalSolidResult(
            solid_def=node_def,
            handle=handle,
            all_events=events_for_handle,
            resources=self._resources,
            resource_defs=self._resource_defs,
            environment_config=self._environment_config,
            pipeline_name=self._pipeline_def.name,
            pipeline_run=self._pipeline_run,
            external_execution_plan=self._external_execution_plan,
        )


class ExternalSolidResult(NodeExecutionResult):
    def __init__(
        self,
        solid_def: SolidDefinition,
        handle: SolidHandle,
        all_events: List[DagsterEvent],
        resources: ScopedResourcesBuilder,
        resource_defs: Dict[str, ResourceDefinition],
        environment_config: EnvironmentConfig,
        pipeline_name: str,
        pipeline_run: PipelineRun,
        external_execution_plan: ExternalExecutionPlan,
    ):
        self._solid_def = solid_def
        self._handle = handle
        self._event_list = all_events
        self._resources = resources
        self._step_events_by_kind = _resolve_step_events_by_kind(self._event_list)
        self._resource_defs = resource_defs
        self._top_level_pipeline_name = pipeline_name
        self._pipeline_run = pipeline_run
        self._external_execution_plan = external_execution_plan
        self._environment_config = environment_config

    @property
    def event_list(self) -> List[DagsterEvent]:
        return self._event_list

    @property
    def handle(self) -> SolidHandle:
        return self._handle

    def _get_value(self, step_output_handle: StepOutputHandle) -> Any:
        output_def = self._solid_def.output_def_named(step_output_handle.output_name)
        io_manager_key = output_def.io_manager_key

        io_manager = check.inst(self._resources.resource_instance_dict[io_manager_key], IOManager)

        required_resource_keys_for_manager = self._resource_defs[
            io_manager_key
        ].required_resource_keys
        resources_for_manager = self._resources.build(required_resource_keys_for_manager)
        resource_config_obj = self._environment_config.resources.get(io_manager_key)
        resource_config = resource_config_obj.config if resource_config_obj else None
        res = io_manager.load_input(
            InputContext(
                name=None,
                pipeline_name=self._top_level_pipeline_name,
                solid_def=self._solid_def,
                config=None,
                metadata=None,
                upstream_output=OutputContext(
                    step_key=step_output_handle.step_key,
                    name=step_output_handle.output_name,
                    pipeline_name=self._top_level_pipeline_name,
                    run_id=self._pipeline_run.run_id,
                    metadata=None,
                    mapping_key=step_output_handle.mapping_key,
                    config=None,
                    solid_def=self._solid_def,
                    dagster_type=output_def.dagster_type,
                    log_manager=initialize_console_manager(self._pipeline_run),
                    version=None,
                    step_context=None,
                    resource_config=resource_config,
                    resources=resources_for_manager,
                ),
                dagster_type=output_def.dagster_type,
                log_manager=initialize_console_manager(self._pipeline_run),
                step_context=None,
                resource_config=resource_config,
                resources=resources_for_manager,
            )
        )
        return res

    @property
    def output_values(self) -> Dict[str, Any]:
        if not self.success:
            raise DagsterInvariantViolationError(
                f"Cannot retrieve output values from solid {self.handle} because execution of the "
                "solid failed."
            )
        results: Dict[str, Any] = {}
        for compute_step_event in self.compute_step_events:
            if compute_step_event.is_successful_output:
                step_snap = self._external_execution_plan.get_step_by_key(
                    compute_step_event.step_key
                )
                step_handle = StepHandle.parse_from_key(step_snap.solid_handle_id)
                mapping_key_from_step = (
                    step_handle.mapping_key
                    if isinstance(step_handle, ResolvedFromDynamicStepHandle)
                    else None
                )
                step_output_handle = compute_step_event.step_output_data.step_output_handle
                value = self._get_value(step_output_handle)
                check.invariant(
                    not (step_output_handle.mapping_key and mapping_key_from_step),
                    "Not set up to handle mapped outputs downstream of mapped steps",
                )
                mapping_key = step_output_handle.mapping_key or mapping_key_from_step
                if mapping_key:
                    if results.get(step_output_handle.output_name) is None:
                        results[step_output_handle.output_name] = {mapping_key: value}
                    else:
                        results[step_output_handle.output_name][mapping_key] = value
                else:
                    results[step_output_handle.output_name] = value

        return results

    @property
    def compute_step_events(self):
        """List[DagsterEvent]: All events generated by execution of the solid compute function."""
        return self._step_events_by_kind.get(StepKind.COMPUTE, [])


class InProcessSolidResult(NodeExecutionResult):
    def __init__(
        self,
        solid_def: SolidDefinition,
        handle: SolidHandle,
        all_events: List[DagsterEvent],
        output_capture: Optional[Dict[StepOutputHandle, Any]],
    ):
        self._solid_def = solid_def
        self._handle = handle
        self._event_list = all_events
        self._output_capture = output_capture

    @property
    def output_values(self) -> Dict[str, Any]:
        solid_handle_as_str = str(self.handle)
        results: Dict[str, Any] = {}
        if self._output_capture:
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

        return results

    @property
    def event_list(self) -> List[DagsterEvent]:
        return self._event_list

    @property
    def handle(self) -> SolidHandle:
        return self._handle


class InProcessGraphResult(NodeExecutionResult):
    def __init__(
        self,
        graph_def: GraphDefinition,
        handle: SolidHandle,
        all_events: List[DagsterEvent],
        output_capture: Optional[Dict[StepOutputHandle, Any]],
    ):
        self._graph_def = graph_def
        self._handle = handle
        self._event_list = all_events
        self._output_capture = output_capture

    def _result_for_handle(self, solid: Solid, handle: SolidHandle) -> NodeExecutionResult:
        node_def = solid.definition
        events_for_handle = _filter_step_events_by_handle(self.event_list, self.handle, handle)
        outputs_for_handle = (
            _filter_outputs_by_handle(self._output_capture, self.handle, handle)
            if self._output_capture
            else None
        )
        handle_with_ancestor = handle.with_ancestor(self.handle)
        if not handle_with_ancestor:
            raise DagsterInvariantViolationError(f"No handle provided for solid {solid.name}")
        if isinstance(node_def, SolidDefinition):
            return InProcessSolidResult(
                solid_def=node_def,
                handle=handle_with_ancestor,
                all_events=events_for_handle,
                output_capture=outputs_for_handle,
            )
        else:
            return InProcessGraphResult(
                graph_def=node_def,
                handle=handle_with_ancestor,
                all_events=events_for_handle,
                output_capture=outputs_for_handle,
            )

    @property
    def output_values(self) -> Dict[str, Any]:
        values = {}

        for output_name in self._graph_def.output_dict:
            output_mapping = self._graph_def.get_output_mapping(output_name)

            inner_solid_values = self._result_for_handle(
                self._graph_def.solid_named(output_mapping.maps_from.solid_name),
                SolidHandle(output_mapping.maps_from.solid_name, None),
            ).output_values

            if inner_solid_values is not None:  # may be None if inner solid was skipped
                if output_mapping.maps_from.output_name in inner_solid_values:
                    values[output_name] = inner_solid_values[output_mapping.maps_from.output_name]

        return values

    @property
    def event_list(self) -> List[DagsterEvent]:
        return self._event_list

    @property
    def handle(self) -> SolidHandle:
        return self._handle

    def result_for_node(self, name: str) -> NodeExecutionResult:
        if not self._graph_def.has_solid_named(name):
            raise DagsterInvariantViolationError(
                "Tried to get result for node '{name}' in '{container}'. No such top level "
                "node.".format(name=name, container=self._graph_def.name)
            )
        handle = SolidHandle(name, None)
        solid = self._graph_def.get_solid(handle)
        return self._result_for_handle(solid, handle)
