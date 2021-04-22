import inspect
from typing import TYPE_CHECKING, Any, Dict, Optional, cast

from dagster import check
from dagster.core.errors import (
    DagsterInvalidConfigError,
    DagsterSolidInvocationError,
    user_code_error_boundary,
)

if TYPE_CHECKING:
    from dagster.core.definitions import SolidDefinition
    from dagster.core.execution.context.invocation import DirectSolidExecutionContext


def solid_invocation_result(
    solid_def: "SolidDefinition", context: Optional["DirectSolidExecutionContext"], *args, **kwargs
) -> Any:

    context = _check_invocation_requirements(solid_def, context)

    input_dict = _resolve_inputs(solid_def, args, kwargs)

    with user_code_error_boundary(
        DagsterSolidInvocationError,
        msg_fn=lambda: f'Error occurred while invoking solid "{solid_def.name}":',
    ):
        output_dict = _execute_and_retrieve_outputs(solid_def, context, input_dict)

        if len(output_dict.keys()) == 1:
            key = list(output_dict.keys())[0]
            return output_dict[key]

        return output_dict


def _check_invocation_requirements(
    solid_def: "SolidDefinition", context: Optional["DirectSolidExecutionContext"]
) -> "DirectSolidExecutionContext":
    """Ensure that provided context fulfills requirements of solid definition.

    If no context was provided, then construct an enpty DirectSolidExecutionContext
    """
    from dagster.core.execution.context.invocation import DirectSolidExecutionContext
    from dagster.config.validate import validate_config

    # Check resource requirements
    if solid_def.required_resource_keys:
        check.invariant(
            context is not None,
            f'Solid "{solid_def.name}" has required resources, but no context was provided. Use the'
            "`get_solid_execution_context` function to construct a context with the required "
            "resources.",
        )
        resources_dict = cast(  # type: ignore[attr-defined]
            "DirectSolidExecutionContext",
            context,
        ).resources._asdict()

        for resource_key in solid_def.required_resource_keys:
            check.invariant(
                resource_key in resources_dict,
                f'Solid "{solid_def.name}" requires resource "{resource_key}", but no resource '
                "with that key was found on the context.",
            )

    # Check config requirements
    if not context:
        check.invariant(
            not solid_def.config_schema.as_field().is_required,
            f'Solid "{solid_def.name}" has required config schema, but no context was provided. '
            "Use the `get_solid_execution_context` function to create a context with config.",
        )

    config = None
    if solid_def.config_field:
        solid_config = check.opt_dict_param(
            context.solid_config if context else None, "solid_config"
        )
        config_evr = validate_config(solid_def.config_field.config_type, solid_config)
        if not config_evr.success:
            raise DagsterInvalidConfigError(
                "Error in config for solid ", config_evr.errors, solid_config
            )
        mapped_config_evr = solid_def.apply_config_mapping({"config": solid_config})
        if not mapped_config_evr.success:
            raise DagsterInvalidConfigError(
                "Error in config for solid ", mapped_config_evr.errors, solid_config
            )
        config = mapped_config_evr.value.get("config", {})
    return (
        context
        if context
        else DirectSolidExecutionContext(solid_config=config, resources=None, instance=None)
    )


def _resolve_inputs(solid_def, args, kwargs):
    input_defs = solid_def.input_defs

    input_dict = {
        input_def.name: input_val for input_val, input_def in zip(args, input_defs[: len(args)])
    }

    for input_def in input_defs[len(args) :]:
        if not input_def.has_default_value:
            check.invariant(
                input_def.name in kwargs,
                f'No value provided for required input "{input_def.name}".',
            )

        input_dict[input_def.name] = (
            kwargs[input_def.name] if input_def.name in kwargs else input_def.default_value
        )

    return input_dict


def _execute_and_retrieve_outputs(
    solid_def: "SolidDefinition", context: "DirectSolidExecutionContext", input_dict: Dict[str, Any]
):
    from dagster.core.execution.plan.compute import gen_from_async_gen

    output_dict = {}

    compute_iterator = solid_def.compute_fn(context, input_dict)

    if inspect.isasyncgen(compute_iterator):
        compute_iterator = gen_from_async_gen(compute_iterator)

    for output in compute_iterator:
        output_dict[output.output_name] = output.value

    return output_dict
