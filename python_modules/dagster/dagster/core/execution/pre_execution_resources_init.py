from collections import deque
from contextlib import contextmanager
from typing import Any, Deque, Dict, Generator, List, Optional, Set

from dagster import check
from dagster.core.definitions.resource import ResourceDefinition, ScopedResourcesBuilder
from dagster.core.errors import DagsterInvariantViolationError, DagsterUserCodeExecutionError
from dagster.core.events import DagsterEvent
from dagster.core.execution.context.init import InitResourceContext
from dagster.core.execution.context_creation_pipeline import (
    ContextCreationData,
    EventGenerationManager,
    create_log_manager,
    executor_def_from_config,
)
from dagster.core.execution.plan.plan import ExecutionPlan
from dagster.core.execution.resources_init import (
    InitializedResource,
    single_resource_generation_manager,
)
from dagster.core.instance import DagsterInstance
from dagster.core.log_manager import DagsterLogManager
from dagster.core.system_config.objects import EnvironmentConfig
from dagster.core.utils import toposort
from dagster.utils.error import serializable_error_info_from_exc_info


def _create_context_creation_data(
    execution_plan: ExecutionPlan,
    environment_config: EnvironmentConfig,
    instance: DagsterInstance,
    run_id: str,
    resource_keys_to_init: Set[str],
) -> ContextCreationData:
    pipeline_def = execution_plan.pipeline.get_definition()

    mode_def = pipeline_def.get_mode_definition(environment_config.mode)
    intermediate_storage_def = environment_config.intermediate_storage_def_for_mode(mode_def)
    executor_def = executor_def_from_config(mode_def, environment_config)

    return ContextCreationData(
        pipeline=execution_plan.pipeline,
        environment_config=environment_config,
        mode_def=mode_def,
        intermediate_storage_def=intermediate_storage_def,
        executor_def=executor_def,
        instance=instance,
        resource_keys_to_init=resource_keys_to_init,
        execution_plan=execution_plan,
        run_id=run_id,
        pipeline_run=None,
    )


def _resolve_resource_dependencies(
    resource_defs: Dict[str, ResourceDefinition], resource_keys_to_init: Set[str]
) -> Dict[str, Set[str]]:
    """Get all resources required to initialize a given set of resources."""
    visited: Set[str] = set()
    to_visit = [key for key in resource_keys_to_init]
    while to_visit:
        cur_key = to_visit.pop()
        visited.add(cur_key)
        resource_def = resource_defs[cur_key]
        for reqd_key in resource_def.required_resource_keys:
            if reqd_key in visited:
                raise DagsterInvariantViolationError(
                    f"Resource key '{reqd_key}' transitively depends on itself."
                )
            to_visit.append(reqd_key)
    return {key: resource_defs[key].required_resource_keys for key in visited}


def _core_resource_initialization_event_generator(
    resource_keys_to_init: Set[str],
    execution_plan: ExecutionPlan,
    resource_managers: Deque[EventGenerationManager],
    resource_log_manager: DagsterLogManager,
    run_id: str,
    instance: DagsterInstance,
):
    pipeline_def = execution_plan.pipeline_def
    resource_instances: Dict[str, ResourceDefinition] = {}
    environment_config = execution_plan.environment_config
    mode_definition = pipeline_def.get_mode_definition(environment_config.mode)
    resource_init_times: Dict[str, Any] = {}
    try:
        yield DagsterEvent.resource_init_start(
            execution_plan,
            resource_log_manager,
            resource_keys_to_init,
        )

        resource_dependencies = _resolve_resource_dependencies(
            mode_definition.resource_defs, resource_keys_to_init
        )
        for level in toposort(resource_dependencies):
            for resource_name in level:
                resource_def = mode_definition.resource_defs[resource_name]
                resource_context = InitResourceContext(
                    resource_def=resource_def,
                    resource_config=environment_config.resources.get(resource_name, {}).get(
                        "config"
                    ),
                    run_id=run_id,
                    environment_config=environment_config,
                    log_manager=resource_log_manager.with_tags(
                        resource_name=resource_name,
                        resource_fn_name=str(resource_def.resource_fn.__name__),
                    ),
                    resource_instance_dict=resource_instances,
                    required_resource_keys=resource_def.required_resource_keys,
                    instance_for_backwards_compat=instance,
                    pipeline_def_for_backwards_compat=pipeline_def,
                )
                manager = single_resource_generation_manager(
                    resource_context, resource_name, resource_def
                )
                for event in manager.generate_setup_events():
                    if event:
                        yield event
                initialized_resource = check.inst(manager.get_object(), InitializedResource)
                resource_instances[resource_name] = initialized_resource.resource
                resource_init_times[resource_name] = initialized_resource.duration
                resource_managers.append(manager)
        yield DagsterEvent.resource_init_success(
            execution_plan, resource_log_manager, resource_instances, resource_init_times
        )
        yield ScopedResourcesBuilder(resource_instances)

    except DagsterUserCodeExecutionError as dagster_user_error:
        yield DagsterEvent.resource_init_failure(
            execution_plan,
            resource_log_manager,
            resource_keys_to_init,
            serializable_error_info_from_exc_info(dagster_user_error.original_exc_info),
        )
        raise dagster_user_error


def resource_initialization_event_generator(
    resource_keys_to_init: Set[str],
    execution_plan: ExecutionPlan,
    run_id: str,
    instance: DagsterInstance,
):
    generator_closed = False
    resource_managers: Deque[EventGenerationManager] = deque()
    context_creation_data = _create_context_creation_data(
        execution_plan, execution_plan.environment_config, instance, run_id, resource_keys_to_init
    )
    log_manager = create_log_manager(context_creation_data)

    try:
        yield from _core_resource_initialization_event_generator(
            resource_keys_to_init=resource_keys_to_init,
            execution_plan=execution_plan,
            resource_managers=resource_managers,
            resource_log_manager=log_manager,
            run_id=run_id,
            instance=instance,
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
                    log_manager,
                    resource_keys_to_init,
                    serializable_error_info_from_exc_info(error.original_exc_info),
                )


def standalone_resources_init_manager(
    resource_keys_to_init: Set[str],
    execution_plan: ExecutionPlan,
    run_id: str,
    instance: DagsterInstance,
) -> EventGenerationManager:
    generator = resource_initialization_event_generator(
        resource_keys_to_init=resource_keys_to_init,
        execution_plan=execution_plan,
        run_id=run_id,
        instance=instance,
    )
    return EventGenerationManager(generator, ScopedResourcesBuilder)


@contextmanager
def init_resources(
    resource_keys_to_init: Set[str],
    execution_plan: ExecutionPlan,
    run_id: str,
    instance: DagsterInstance,
    recorder: Optional[List[DagsterEvent]] = None,
) -> Generator[ScopedResourcesBuilder, None, None]:
    resources_manager = standalone_resources_init_manager(
        resource_keys_to_init=resource_keys_to_init,
        execution_plan=execution_plan,
        run_id=run_id,
        instance=instance,
    )
    try:
        _setup_events = list(resources_manager.generate_setup_events())
        if recorder:
            for event in _setup_events:
                recorder.append(event)
        resources = check.inst(resources_manager.get_object(), ScopedResourcesBuilder)
        yield resources
    finally:
        _teardown_events = resources_manager.generate_teardown_events()
        if recorder:
            for event in _teardown_events:
                recorder.append(_teardown_events)
