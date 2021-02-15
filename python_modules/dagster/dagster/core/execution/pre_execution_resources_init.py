from collections import deque
from contextlib import contextmanager
from typing import Any, Deque, Dict, Generator, List, Optional, Set

from dagster import check
from dagster.core.definitions.resource import ResourceDefinition, ScopedResourcesBuilder
from dagster.core.errors import DagsterInvariantViolationError, DagsterUserCodeExecutionError
from dagster.core.events import DagsterEvent
from dagster.core.execution.context.init import InitResourceContext
from dagster.core.execution.context.logger import InitLoggerContext
from dagster.core.execution.context_creation_pipeline import EventGenerationManager
from dagster.core.execution.resources_init import (
    InitializedResource,
    single_resource_generation_manager,
)
from dagster.core.log_manager import DagsterLogManager
from dagster.core.storage.pipeline_run import PipelineRun
from dagster.core.system_config.objects import EnvironmentConfig
from dagster.core.utils import toposort
from dagster.loggers import default_system_loggers


def _resolve_resource_dependencies(
    resource_defs: Dict[str, ResourceDefinition], resource_keys_to_init: Optional[Set[str]]
) -> Dict[str, Set[str]]:
    """Get all resources required to initialize a given set of resources."""
    visited: Set[str] = set()
    to_visit = [
        key for key in (resource_keys_to_init if resource_keys_to_init else resource_defs.keys())
    ]
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
    resource_defs: Dict[str, ResourceDefinition],
    environment_config: EnvironmentConfig,
    resource_managers: Deque[EventGenerationManager],
    resource_log_manager: DagsterLogManager,
    resource_keys_to_init: Optional[Set[str]],
    pipeline_run: Optional[PipelineRun],
):
    resource_instances: Dict[str, InitializedResource] = {}
    resource_init_times: Dict[str, Any] = {}
    try:

        resource_dependencies = _resolve_resource_dependencies(resource_defs, resource_keys_to_init)
        for level in toposort(resource_dependencies):
            for resource_name in level:
                resource_def = resource_defs[resource_name]
                resource_context = InitResourceContext(
                    resource_config=environment_config.resources.get(resource_name, {}).get(
                        "config"
                    ),
                    resource_def=resource_def,
                    environment_config=environment_config,
                    log_manager=resource_log_manager.with_tags(
                        resource_name=resource_name,
                        resource_fn_name=str(resource_def.resource_fn.__name__),
                    ),
                    resource_instance_dict=resource_instances,
                    required_resource_keys=resource_def.required_resource_keys,
                    pipeline_run=pipeline_run,
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
        yield ScopedResourcesBuilder(resource_instances)

    except DagsterUserCodeExecutionError as dagster_user_error:
        raise dagster_user_error


def _initialize_console_manager(pipeline_run: Optional[PipelineRun]) -> DagsterLogManager:
    # initialize default colored console logger
    loggers = []
    for logger_def, logger_config in default_system_loggers():
        loggers.append(
            logger_def.logger_fn(
                InitLoggerContext(
                    logger_config, logger_def, run_id=pipeline_run.run_id if pipeline_run else None
                )
            )
        )
    return DagsterLogManager(
        None, pipeline_run.tags if pipeline_run and pipeline_run.tags else {}, loggers
    )


def resource_initialization_event_generator(
    resource_defs: Dict[str, ResourceDefinition],
    environment_config: EnvironmentConfig,
    resource_keys_to_init: Optional[Set[str]],
    pipeline_run: Optional[PipelineRun],
):
    generator_closed = False
    resource_managers: Deque[EventGenerationManager] = deque()
    console_log_manager = _initialize_console_manager(pipeline_run)

    try:
        yield from _core_resource_initialization_event_generator(
            resource_defs=resource_defs,
            environment_config=environment_config,
            resource_managers=resource_managers,
            resource_log_manager=console_log_manager,
            resource_keys_to_init=resource_keys_to_init,
            pipeline_run=pipeline_run,
        )
    except GeneratorExit:
        # Shouldn't happen, but avoid runtime-exception in case this generator gets GC-ed
        # (see https://amir.rachum.com/blog/2017/03/03/generator-cleanup/).
        generator_closed = True
        raise
    finally:
        if not generator_closed:
            while len(resource_managers) > 0:
                manager = resource_managers.pop()
                yield from manager.generate_teardown_events()


def standalone_resources_init_manager(
    resource_defs: Dict[str, ResourceDefinition],
    environment_config: EnvironmentConfig,
    resource_keys_to_init: Optional[Set[str]],
    pipeline_run: Optional[PipelineRun],
) -> EventGenerationManager:
    generator = resource_initialization_event_generator(
        resource_defs=resource_defs,
        environment_config=environment_config,
        resource_keys_to_init=resource_keys_to_init,
        pipeline_run=pipeline_run,
    )
    return EventGenerationManager(generator, ScopedResourcesBuilder)


@contextmanager
def init_resources(
    resource_defs: Dict[str, ResourceDefinition],
    environment_config: EnvironmentConfig,
    resource_keys_to_init: Optional[Set[str]] = None,
    pipeline_run: Optional[PipelineRun] = None,
    recorder: Optional[List[DagsterEvent]] = None,
) -> Generator[ScopedResourcesBuilder, None, None]:
    resources_manager = standalone_resources_init_manager(
        resource_defs=resource_defs,
        environment_config=environment_config,
        resource_keys_to_init=resource_keys_to_init,
        pipeline_run=pipeline_run,
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
