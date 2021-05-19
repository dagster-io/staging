import inspect
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional, cast

from dagster import check
from dagster.core.errors import DagsterInvalidConfigError, DagsterInvalidInvocationError

if TYPE_CHECKING:
    from dagster.core.definitions.resource import ResourceDefinition
    from dagster.core.execution.context.init import InitResourceContext
    from dagster.config.field import Field


def resource_invocation_result(
    resource_def: "ResourceDefinition", init_context: Optional["InitResourceContext"]
) -> Any:
    if not resource_def.resource_fn:
        return None
    init_context = _check_invocation_requirements(resource_def, init_context)

    val_or_gen = resource_def.resource_fn(init_context)
    if inspect.isgenerator(val_or_gen):

        @contextmanager
        def _wrap_gen():
            try:
                val = next(val_or_gen)
                yield val
            except StopIteration:
                check.failed("Resource generator must yield one item.")

        return _wrap_gen()
    else:
        return val_or_gen


def _check_invocation_requirements(
    resource_def: "ResourceDefinition", init_context: Optional["InitResourceContext"]
) -> "InitResourceContext":
    from dagster.config.validate import validate_config
    from dagster.core.execution.context_creation_pipeline import initialize_console_manager
    from dagster.core.execution.context.init import InitResourceContext
    from dagster.core.definitions.resource import ScopedResourcesBuilder

    if resource_def.required_resource_keys and init_context is None:
        raise DagsterInvalidInvocationError(
            "Resource has required resources, but no context was provided. Use the "
            "`build_resource_init_context` function to construct a context with the required "
            "resources."
        )

    if init_context is not None and resource_def.required_resource_keys:
        resources_dict = cast(  # type: ignore[attr-defined]
            "InitResourceContext",
            init_context,
        ).resources._asdict()

        for resource_key in resource_def.required_resource_keys:
            if resource_key not in resources_dict:
                raise DagsterInvalidInvocationError(
                    f'Resource requires resource "{resource_key}", but no resource '
                    "with that key was found on the context."
                )

    # Check config requirements
    if not init_context and resource_def.config_schema.as_field().is_required:
        raise DagsterInvalidInvocationError(
            "Resource has required config schema, but no context was provided. "
            "Use the `build_resource_init_context` function to create a context with config."
        )

    config = None
    if resource_def.has_config_field:
        resource_config = check.opt_dict_param(
            init_context.resource_config if init_context else None, "init_context"
        )
        config_field = cast("Field", resource_def.config_field)
        config_evr = validate_config(config_field.config_type, resource_config)
        if not config_evr.success:
            raise DagsterInvalidConfigError(
                "Error in config for resource ", config_evr.errors, resource_config
            )
        mapped_config_evr = resource_def.apply_config_mapping({"config": resource_config})
        if not mapped_config_evr.success:
            raise DagsterInvalidConfigError(
                "Error in config for resource ", mapped_config_evr.errors, resource_config
            )
        config = mapped_config_evr.value.get("config", {})
    if init_context:
        return InitResourceContext(
            resource_config=init_context.resource_config,
            resource_def=resource_def,
            pipeline_run=None,
            log_manager=init_context.log,
            resources=init_context.resources,
            instance=init_context.instance,
        )
    else:
        return InitResourceContext(
            resource_config=config,
            resource_def=resource_def,
            pipeline_run=None,
            log_manager=initialize_console_manager(None),
            resources=ScopedResourcesBuilder().build(None),
            instance=None,
            pipeline_def_for_backwards_compat=None,
        )
