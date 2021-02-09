from collections import defaultdict, deque
from contextlib import contextmanager

from dagster import check
from dagster.core.definitions import (
    GraphDefinition,
    PipelineDefinition,
    Solid,
    SolidDefinition,
    SolidHandle,
)
from dagster.core.definitions.resource import ScopedResourcesBuilder
from dagster.core.definitions.utils import DEFAULT_OUTPUT
from dagster.core.errors import DagsterInvariantViolationError, DagsterUserCodeExecutionError
from dagster.core.events import DagsterEvent, DagsterEventType
from dagster.core.execution.plan.step import StepKind
from dagster.core.log_manager import DagsterLogManager
from dagster.core.utils import toposort
from dagster.loggers import default_system_loggers
from dagster.utils import EventGenerationManager

from .context.init import InitResourceContext
from .context.logger import InitLoggerContext
from .context.system import InputContext, OutputContext
from .resources_init import InitializedResource, single_resource_event_generator


def create_log_manager(pipeline_def, mode_def, execution_plan, run_id):
    environment_config = execution_plan.environment_config

    # The following logic is tightly coupled to the processing of logger config in
    # python_modules/dagster/dagster/core/system_config/objects.py#config_map_loggers
    # Changes here should be accompanied checked against that function, which applies config mapping
    # via ConfigurableDefinition (@configured) to incoming logger configs. See docstring for more details.

    loggers = []
    for logger_key, logger_def in mode_def.loggers.items() or default_loggers().items():
        if logger_key in environment_config.loggers:
            loggers.append(
                logger_def.logger_fn(
                    InitLoggerContext(
                        environment_config.loggers.get(logger_key, {}).get("config"),
                        pipeline_def,
                        logger_def,
                        run_id,
                    )
                )
            )

    if not loggers:
        for (logger_def, logger_config) in default_system_loggers():
            loggers.append(
                logger_def.logger_fn(
                    InitLoggerContext(logger_config, pipeline_def, logger_def, run_id)
                )
            )

    return DagsterLogManager(
        run_id=run_id,
        logging_tags={},
        loggers=loggers,
    )


def _construct_events_by_step_key(event_list):
    events_by_step_key = defaultdict(list)
    for event in event_list:
        events_by_step_key[event.step_key].append(event)

    return dict(events_by_step_key)


def _resolve_resource_dependencies(resource_defs):
    """Generates a dictionary that maps resource key to resource keys it requires for initialization"""
    resource_dependencies = {
        key: resource_def.required_resource_keys for key, resource_def in resource_defs.items()
    }
    return resource_dependencies


def _single_resource_generation_manager(context, resource_name, resource_def):
    generator = single_resource_event_generator(context, resource_name, resource_def)
    return EventGenerationManager(generator, InitializedResource)


def _core_resource_generation(execution_plan, run_id, resource_defs, resource_managers, mode_def):
    resource_deps = _resolve_resource_dependencies(resource_defs)
    log_manager = create_log_manager(execution_plan.pipeline_def, mode_def, execution_plan, run_id)
    resource_instances = {}
    resource_init_times = {}
    resource_managers = deque()
    try:
        yield DagsterEvent.resource_init_start(
            execution_plan,
            log_manager,
            resource_defs.keys(),
        )
        for level in toposort(resource_deps):
            for resource_name in level:
                resource_def = mode_def.resource_defs[resource_name]
                resource_context = InitResourceContext(
                    resource_def=resource_def,
                    resource_config=execution_plan.environment_config.resources.get(
                        resource_name, {}
                    ).get("config"),
                    run_id=run_id,
                    resource_instance_dict=resource_instances,
                    required_resource_keys=resource_def.required_resource_keys,
                )
                manager = _single_resource_generation_manager(
                    resource_context, resource_name, resource_def
                )
                for event in manager.generate_setup_events():
                    if event:
                        yield event
                initialized_resource = check.inst(manager.get_object(), InitializedResource)
                resource_instances[resource_name] = initialized_resource.resource
                resource_init_times[resource_name] = initialized_resource.duration
                resource_managers.append(manager)
        yield ScopedResourcesBuilder(resource_instances)
    except DagsterUserCodeExecutionError as dagster_user_error:
        yield DagsterEvent.resource_init_failure(
            execution_plan,
            log_manager,
            resource_keys_to_init,
            serializable_error_info_from_exc_info(dagster_user_error.original_exc_info),
        )
        raise dagster_user_error


def generate_resource_from_key(execution_plan, run_id):
    mode_def = execution_plan.pipeline_def.get_mode_definition("created")
    resource_defs = mode_def.resource_defs
    generator_closed = False
    resource_managers = deque()
    try:
        yield from _core_resource_generation(
            execution_plan, run_id, resource_defs, resource_managers, mode_def
        )
    except GeneratorExit:
        # Shouldn't happen, but avoid runtime-exception in case this generator gets GC-ed
        # (see https://amir.rachum.com/blog/2017/03/03/generator-cleanup/).
        generator_closed = True
        raise
    finally:
        if not generator_closed:
            error = None
            while len(resource_managers) > 0:
                manager = resource_managers.pop()
                try:
                    yield from manager.generate_teardown_events()
                except DagsterUserCodeExecutionError as dagster_user_error:
                    error = dagster_user_error
            if error:
                yield DagsterEvent.resource_teardown_failure(
                    execution_plan,
                    None,
                    resource_keys_to_init,
                    serializable_error_info_from_exc_info(error.original_exc_info),
                )


def standalone_resource_init_manager(execution_plan, run_id):
    generator = generate_resource_from_key(execution_plan, run_id)
    return EventGenerationManager(generator, ScopedResourcesBuilder)


@contextmanager
def initialize_resources(execution_plan, run_id):
    resources_manager = standalone_resource_init_manager(execution_plan, run_id)
    try:
        _events = list(resources_manager.generate_setup_events())
        scoped_resources_builder = check.inst(
            resources_manager.get_object(), ScopedResourcesBuilder
        )
        yield scoped_resources_builder
    finally:
        _events = resources_manager.generate_teardown_events()


class ExecutionResult:
    def __init__(
        self,
        node_def,
        event_list,
        execution_plan,
        run_id,
        reconstruct_context,
        resource_instances_to_override=None,
        handle=None,
    ):
        self.node_def = node_def
        self.event_list = event_list
        self._reconstruct_context = reconstruct_context
        self._resource_instances_to_override = resource_instances_to_override
        self._handle = handle
        self._execution_plan = execution_plan
        self._run_id = run_id

        events_by_kind = defaultdict(list)
        for event in self.event_list:
            if event.is_step_event:
                events_by_kind[event.step_kind].append(event)
        self._step_events_by_kind = events_by_kind

    def _resource_key_to_instance(self, resource_key):
        return _generate_resource_from_key(resource_key, self._execution_plan, self._run_id)

    @property
    def step_events_by_kind(self):
        return self._step_events_by_kind

    @property
    def handle(self):
        return self._handle

    @property
    def compute_step_events(self):
        """List[DagsterEvent]: All events generated by execution of the solid compute function."""
        return self.step_events_by_kind.get(StepKind.COMPUTE, [])

    @property
    def reconstruct_context(self):
        return self._reconstruct_context

    @property
    def resource_instances_to_override(self):
        return self._resource_instances_to_override

    @property
    def success(self):
        """bool: Whether all steps in the execution were successful."""
        return all([not event.is_failure for event in self.event_list])

    @property
    def output_values(self):
        results = {}
        with initialize_resources(self._execution_plan, self._run_id) as scoped_resources:
            for compute_step_event in self.compute_step_events:
                if compute_step_event.is_successful_output:
                    output = compute_step_event.step_output_data
                    step = self._execution_plan.get_step_by_key(compute_step_event.step_key)
                    value = self._get_value(
                        scoped_resources.resource_instance_dict, self._execution_plan, output
                    )
                    check.invariant(
                        not (output.mapping_key and step.get_mapping_key()),
                        "Not set up to handle mapped outputs downstream of mapped steps",
                    )
                    mapping_key = output.mapping_key or step.get_mapping_key()
                    if mapping_key:
                        if results.get(output.output_name) is None:
                            results[output.output_name] = {mapping_key: value}
                        else:
                            results[output.output_name][mapping_key] = value
                    else:
                        results[output.output_name] = value

        return results

    def _get_value(self, resources, execution_plan, step_output_data):
        step_output_handle = step_output_data.step_output_handle
        step_output = execution_plan.get_step_by_key(step_output_handle.step_key)
        solid_handle = step_output.solid_handle
        manager_key = execution_plan.get_manager_key(step_output_handle)
        manager = resources[manager_key]
        solid_def = execution_plan.pipeline_def.solid_named(solid_handle.name).definition

        res = manager.load_input(
            InputContext(
                execution_plan.pipeline_def.name,
                solid_def=solid_def,
                config=None,
                metadata=None,
                upstream_output=OutputContext(
                    step_output_handle.step_key,
                    step_output_handle.output_name,
                    execution_plan.pipeline_def.name,
                    self._run_id,
                ),
            )
        )
        return res

    def result_for_handle(self, handle):
        """Get the result of a top level node.

        Args:
            handle (Union[str, SolidHandle]): The handle of the top-level node for which to retrieve
                the result.

        Returns:
            NodeExecutionResult: The result of the node's execution within this node.
        """
        if isinstance(self.node_def, SolidDefinition):
            raise DagsterInvariantViolationError("Cannot retrieve an inner result from a solid.")

        if isinstance(handle, str):
            handle = SolidHandle.from_string(handle)
        else:
            check.inst_param(handle, "handle", SolidHandle)

        node = self.node_def.get_solid(handle)

        events = [
            event
            for event in self.event_list
            if event.is_step_event
            and event.solid_handle.is_or_descends_from(handle.with_ancestor(self.handle))
        ]

        return ExecutionResult(
            node.definition,
            events,
            self._reconstruct_context,
            resource_instances_to_override=self.resource_instances_to_override,
            handle=handle.with_ancestor(self.handle),
        )


class GraphExecutionResult:
    def __init__(
        self,
        container,
        event_list,
        reconstruct_context,
        handle=None,
        resource_instances_to_override=None,
    ):
        self.container = check.inst_param(container, "container", GraphDefinition)
        self.event_list = check.list_param(event_list, "step_event_list", of_type=DagsterEvent)
        self.reconstruct_context = check.callable_param(reconstruct_context, "reconstruct_context")
        self.handle = check.opt_inst_param(handle, "handle", SolidHandle)
        self.resource_instances_to_override = check.opt_dict_param(
            resource_instances_to_override, "resource_instances_to_override", str
        )
        self._events_by_step_key = _construct_events_by_step_key(event_list)

    @property
    def success(self):
        """bool: Whether all steps in the execution were successful."""
        return all([not event.is_failure for event in self.event_list])

    @property
    def step_event_list(self):
        """List[DagsterEvent] The full list of events generated by steps in the execution.

        Excludes events generated by the pipeline lifecycle, e.g., ``PIPELINE_START``.
        """
        return [event for event in self.event_list if event.is_step_event]

    @property
    def events_by_step_key(self):
        return self._events_by_step_key

    def result_for_solid(self, name):
        """Get the result of a top level solid.

        Args:
            name (str): The name of the top-level solid or aliased solid for which to retrieve the
                result.

        Returns:
            Union[CompositeSolidExecutionResult, SolidExecutionResult]: The result of the solid
            execution within the pipeline.
        """
        if not self.container.has_solid_named(name):
            raise DagsterInvariantViolationError(
                "Tried to get result for solid '{name}' in '{container}'. No such top level "
                "solid.".format(name=name, container=self.container.name)
            )

        return self.result_for_handle(SolidHandle(name, None))

    def output_for_solid(self, handle_str, output_name=DEFAULT_OUTPUT):
        """Get the output of a solid by its solid handle string and output name.

        Args:
            handle_str (str): The string handle for the solid.
            output_name (str): Optional. The name of the output, default to DEFAULT_OUTPUT.

        Returns:
            The output value for the handle and output_name.
        """
        check.str_param(handle_str, "handle_str")
        check.str_param(output_name, "output_name")
        return self.result_for_handle(SolidHandle.from_string(handle_str)).output_value(output_name)

    @property
    def solid_result_list(self):
        """List[Union[CompositeSolidExecutionResult, SolidExecutionResult]]: The results for each
        top level solid."""
        return [self.result_for_solid(solid.name) for solid in self.container.solids]

    def _result_for_handle(self, solid, handle):
        if not solid:
            raise DagsterInvariantViolationError(
                "Can not find solid handle {handle_str}.".format(handle_str=handle.to_string())
            )

        events_by_kind = defaultdict(list)

        if solid.is_composite:
            events = []
            for event in self.event_list:
                if event.is_step_event:
                    if event.solid_handle.is_or_descends_from(handle.with_ancestor(self.handle)):
                        events_by_kind[event.step_kind].append(event)
                        events.append(event)

            return CompositeSolidExecutionResult(
                solid,
                events,
                events_by_kind,
                self.reconstruct_context,
                handle=handle.with_ancestor(self.handle),
                resource_instances_to_override=self.resource_instances_to_override,
            )
        else:
            for event in self.event_list:
                if event.is_step_event:
                    if event.solid_handle.is_or_descends_from(handle.with_ancestor(self.handle)):
                        events_by_kind[event.step_kind].append(event)

            return SolidExecutionResult(
                solid,
                events_by_kind,
                self.reconstruct_context,
                resource_instances_to_override=self.resource_instances_to_override,
            )

    def result_for_handle(self, handle):
        """Get the result of a solid by its solid handle.

        This allows indexing into top-level solids to retrieve the results of children of
        composite solids.

        Args:
            handle (Union[str,SolidHandle]): The handle for the solid.

        Returns:
            Union[CompositeSolidExecutionResult, SolidExecutionResult]: The result of the given
            solid.
        """
        if isinstance(handle, str):
            handle = SolidHandle.from_string(handle)
        else:
            check.inst_param(handle, "handle", SolidHandle)

        solid = self.container.get_solid(handle)

        return self._result_for_handle(solid, handle)


class PipelineExecutionResult(GraphExecutionResult):
    """The result of executing a pipeline.

    Returned by :py:func:`execute_pipeline`. Users should not instantiate this class.
    """

    def __init__(
        self,
        pipeline_def,
        run_id,
        event_list,
        reconstruct_context,
        resource_instances_to_override=None,
    ):
        self.run_id = check.str_param(run_id, "run_id")
        check.inst_param(pipeline_def, "pipeline_def", PipelineDefinition)

        super(PipelineExecutionResult, self).__init__(
            container=pipeline_def,
            event_list=event_list,
            reconstruct_context=reconstruct_context,
            resource_instances_to_override=resource_instances_to_override,
        )

    @property
    def pipeline_def(self):
        return self.container


class CompositeSolidExecutionResult(GraphExecutionResult):
    """Execution result for a composite solid in a pipeline.

    Users should not instantiate this class.
    """

    def __init__(
        self,
        solid,
        event_list,
        step_events_by_kind,
        reconstruct_context,
        handle=None,
        resource_instances_to_override=None,
    ):
        check.inst_param(solid, "solid", Solid)
        check.invariant(
            solid.is_composite,
            desc="Tried to instantiate a CompositeSolidExecutionResult with a noncomposite solid",
        )
        self.solid = solid
        self.step_events_by_kind = check.dict_param(
            step_events_by_kind, "step_events_by_kind", key_type=StepKind, value_type=list
        )
        self.resource_instances_to_override = check.opt_dict_param(
            resource_instances_to_override, "resource_instances_to_override", str
        )
        super(CompositeSolidExecutionResult, self).__init__(
            container=solid.definition,
            event_list=event_list,
            reconstruct_context=reconstruct_context,
            handle=handle,
            resource_instances_to_override=resource_instances_to_override,
        )

    def output_values_for_solid(self, name):
        check.str_param(name, "name")
        return self.result_for_solid(name).output_values

    def output_values_for_handle(self, handle_str):
        check.str_param(handle_str, "handle_str")

        return self.result_for_handle(handle_str).output_values

    def output_value_for_solid(self, name, output_name=DEFAULT_OUTPUT):
        check.str_param(name, "name")
        check.str_param(output_name, "output_name")

        return self.result_for_solid(name).output_value(output_name)

    def output_value_for_handle(self, handle_str, output_name=DEFAULT_OUTPUT):
        check.str_param(handle_str, "handle_str")
        check.str_param(output_name, "output_name")

        return self.result_for_handle(handle_str).output_value(output_name)

    @property
    def output_values(self):
        values = {}

        for output_name in self.solid.definition.output_dict:
            output_mapping = self.solid.definition.get_output_mapping(output_name)

            inner_solid_values = self._result_for_handle(
                self.solid.definition.solid_named(output_mapping.maps_from.solid_name),
                SolidHandle(output_mapping.maps_from.solid_name, None),
            ).output_values

            if inner_solid_values is not None:  # may be None if inner solid was skipped
                if output_mapping.maps_from.output_name in inner_solid_values:
                    values[output_name] = inner_solid_values[output_mapping.maps_from.output_name]

        return values

    def output_value(self, output_name=DEFAULT_OUTPUT):
        check.str_param(output_name, "output_name")

        if not self.solid.definition.has_output(output_name):
            raise DagsterInvariantViolationError(
                "Output '{output_name}' not defined in composite solid '{solid}': "
                "{outputs_clause}. If you were expecting this output to be present, you may "
                "be missing an output_mapping from an inner solid to its enclosing composite "
                "solid.".format(
                    output_name=output_name,
                    solid=self.solid.name,
                    outputs_clause="found outputs {output_names}".format(
                        output_names=str(list(self.solid.definition.output_dict.keys()))
                    )
                    if self.solid.definition.output_dict
                    else "no output mappings were defined",
                )
            )

        output_mapping = self.solid.definition.get_output_mapping(output_name)

        return self._result_for_handle(
            self.solid.definition.solid_named(output_mapping.maps_from.solid_name),
            SolidHandle(output_mapping.maps_from.solid_name, None),
        ).output_value(output_mapping.maps_from.output_name)


class SolidExecutionResult:
    """Execution result for a leaf solid in a pipeline.

    Users should not instantiate this class.
    """

    def __init__(
        self, solid, step_events_by_kind, reconstruct_context, resource_instances_to_override=None
    ):
        check.inst_param(solid, "solid", Solid)
        check.invariant(
            not solid.is_composite,
            desc="Tried to instantiate a SolidExecutionResult with a composite solid",
        )
        self.solid = solid
        self.step_events_by_kind = check.dict_param(
            step_events_by_kind, "step_events_by_kind", key_type=StepKind, value_type=list
        )
        self.reconstruct_context = check.callable_param(reconstruct_context, "reconstruct_context")
        self.resource_instances_to_override = check.opt_dict_param(
            resource_instances_to_override, "resource_instances_to_override", str
        )

    @property
    def compute_input_event_dict(self):
        """Dict[str, DagsterEvent]: All events of type ``STEP_INPUT``, keyed by input name."""
        return {se.event_specific_data.input_name: se for se in self.input_events_during_compute}

    @property
    def input_events_during_compute(self):
        """List[DagsterEvent]: All events of type ``STEP_INPUT``."""
        return self._compute_steps_of_type(DagsterEventType.STEP_INPUT)

    def get_output_event_for_compute(self, output_name="result"):
        """The ``STEP_OUTPUT`` event for the given output name.

        Throws if not present.

        Args:
            output_name (Optional[str]): The name of the output. (default: 'result')

        Returns:
            DagsterEvent: The corresponding event.
        """
        events = self.get_output_events_for_compute(output_name)
        check.invariant(
            len(events) == 1, "Multiple output events returned, use get_output_events_for_compute"
        )
        return events[0]

    @property
    def compute_output_events_dict(self):
        """Dict[str, List[DagsterEvent]]: All events of type ``STEP_OUTPUT``, keyed by output name"""
        results = defaultdict(list)
        for se in self.output_events_during_compute:
            results[se.step_output_data.output_name].append(se)

        return dict(results)

    def get_output_events_for_compute(self, output_name="result"):
        """The ``STEP_OUTPUT`` event for the given output name.

        Throws if not present.

        Args:
            output_name (Optional[str]): The name of the output. (default: 'result')

        Returns:
            List[DagsterEvent]: The corresponding events.
        """
        return self.compute_output_events_dict[output_name]

    @property
    def output_events_during_compute(self):
        """List[DagsterEvent]: All events of type ``STEP_OUTPUT``."""
        return self._compute_steps_of_type(DagsterEventType.STEP_OUTPUT)

    @property
    def compute_step_events(self):
        """List[DagsterEvent]: All events generated by execution of the solid compute function."""
        return self.step_events_by_kind.get(StepKind.COMPUTE, [])

    @property
    def step_events(self):
        return self.compute_step_events

    @property
    def materializations_during_compute(self):
        """List[Materialization]: All materializations yielded by the solid."""
        return [
            mat_event.event_specific_data.materialization
            for mat_event in self.materialization_events_during_compute
        ]

    @property
    def materialization_events_during_compute(self):
        """List[DagsterEvent]: All events of type ``STEP_MATERIALIZATION``."""
        return self._compute_steps_of_type(DagsterEventType.STEP_MATERIALIZATION)

    @property
    def expectation_events_during_compute(self):
        """List[DagsterEvent]: All events of type ``STEP_EXPECTATION_RESULT``."""
        return self._compute_steps_of_type(DagsterEventType.STEP_EXPECTATION_RESULT)

    def _compute_steps_of_type(self, dagster_event_type):
        return list(
            filter(lambda se: se.event_type == dagster_event_type, self.compute_step_events)
        )

    @property
    def expectation_results_during_compute(self):
        """List[ExpectationResult]: All expectation results yielded by the solid"""
        return [
            expt_event.event_specific_data.expectation_result
            for expt_event in self.expectation_events_during_compute
        ]

    def get_step_success_event(self):
        """DagsterEvent: The ``STEP_SUCCESS`` event, throws if not present."""
        for step_event in self.compute_step_events:
            if step_event.event_type == DagsterEventType.STEP_SUCCESS:
                return step_event

        check.failed("Step success not found for solid {}".format(self.solid.name))

    @property
    def compute_step_failure_event(self):
        """DagsterEvent: The ``STEP_FAILURE`` event, throws if it did not fail."""
        if self.success:
            raise DagsterInvariantViolationError(
                "Cannot call compute_step_failure_event if successful"
            )

        step_failure_events = self._compute_steps_of_type(DagsterEventType.STEP_FAILURE)
        check.invariant(len(step_failure_events) == 1)
        return step_failure_events[0]

    @property
    def success(self):
        """bool: Whether solid execution was successful."""
        any_success = False
        for step_event in self.compute_step_events:
            if step_event.event_type == DagsterEventType.STEP_FAILURE:
                return False
            if step_event.event_type == DagsterEventType.STEP_SUCCESS:
                any_success = True

        return any_success

    @property
    def skipped(self):
        """bool: Whether solid execution was skipped."""
        return all(
            [
                step_event.event_type == DagsterEventType.STEP_SKIPPED
                for step_event in self.compute_step_events
            ]
        )

    @property
    def output_values(self):
        """Union[None, Dict[str, Union[Any, Dict[str, Any]]]: The computed output values.

        Returns ``None`` if execution did not succeed.

        Returns a dictionary where keys are output names and the values are:
            * the output values in the normal case
            * a dictionary from mapping key to corresponding value in the mapped case

        Note that accessing this property will reconstruct the pipeline context (including, e.g.,
        resources) to retrieve materialized output values.
        """
        if not self.success or not self.compute_step_events:
            return None

        results = {}
        with self.reconstruct_context(self.resource_instances_to_override) as context:
            for compute_step_event in self.compute_step_events:
                if compute_step_event.is_successful_output:
                    output = compute_step_event.step_output_data
                    step = context.execution_plan.get_step_by_key(compute_step_event.step_key)
                    value = self._get_value(context.for_step(step), output)
                    check.invariant(
                        not (output.mapping_key and step.get_mapping_key()),
                        "Not set up to handle mapped outputs downstream of mapped steps",
                    )
                    mapping_key = output.mapping_key or step.get_mapping_key()
                    if mapping_key:
                        if results.get(output.output_name) is None:
                            results[output.output_name] = {mapping_key: value}
                        else:
                            results[output.output_name][mapping_key] = value
                    else:
                        results[output.output_name] = value

        return results

    def output_value(self, output_name=DEFAULT_OUTPUT):
        """Get a computed output value.

        Note that calling this method will reconstruct the pipeline context (including, e.g.,
        resources) to retrieve materialized output values.

        Args:
            output_name(str): The output name for which to retrieve the value. (default: 'result')

        Returns:
            Union[None, Any, Dict[str, Any]]: ``None`` if execution did not succeed, the output value
                in the normal case, and a dict of mapping keys to values in the mapped case.
        """
        check.str_param(output_name, "output_name")

        if not self.solid.definition.has_output(output_name):
            raise DagsterInvariantViolationError(
                "Output '{output_name}' not defined in solid '{solid}': found outputs "
                "{output_names}".format(
                    output_name=output_name,
                    solid=self.solid.name,
                    output_names=str(list(self.solid.definition.output_dict.keys())),
                )
            )

        if not self.success:
            return None

        with self.reconstruct_context(self.resource_instances_to_override) as context:
            found = False
            result = None
            for compute_step_event in self.compute_step_events:
                if (
                    compute_step_event.is_successful_output
                    and compute_step_event.step_output_data.output_name == output_name
                ):
                    found = True
                    output = compute_step_event.step_output_data
                    step = context.execution_plan.get_step_by_key(compute_step_event.step_key)
                    value = self._get_value(context.for_step(step), output)
                    check.invariant(
                        not (output.mapping_key and step.get_mapping_key()),
                        "Not set up to handle mapped outputs downstream of mapped steps",
                    )
                    mapping_key = output.mapping_key or step.get_mapping_key()
                    if mapping_key:
                        if result is None:
                            result = {mapping_key: value}
                        else:
                            result[mapping_key] = value
                    else:
                        result = value

            if found:
                return result

            raise DagsterInvariantViolationError(
                (
                    "Did not find result {output_name} in solid {self.solid.name} "
                    "execution result"
                ).format(output_name=output_name, self=self)
            )

    def _get_value(self, context, step_output_data):
        step_output_handle = step_output_data.step_output_handle
        manager = context.get_io_manager(step_output_handle)

        res = manager.load_input(
            context.for_input_manager(
                name=None,
                config=None,
                metadata=None,
                dagster_type=self.solid.output_def_named(step_output_data.output_name).dagster_type,
                source_handle=step_output_handle,
            )
        )
        return res

    @property
    def failure_data(self):
        """Union[None, StepFailureData]: Any data corresponding to this step's failure, if it
        failed."""
        for step_event in self.compute_step_events:
            if step_event.event_type == DagsterEventType.STEP_FAILURE:
                return step_event.step_failure_data
