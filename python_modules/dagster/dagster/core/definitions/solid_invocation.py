from typing import TYPE_CHECKING, Any, Optional, Union

from dagster.core.errors import DagsterInvalidInvocationError, DagsterTypeCheckDidNotPass

if TYPE_CHECKING:
    from dagster.core.definitions import SolidDefinition
    from dagster.core.definitions.composition import PendingNodeInvocation
    from dagster.core.execution.context.invocation import (
        BoundSolidExecutionContext,
        UnboundSolidExecutionContext,
    )


def solid_invocation_result(
    solid_def_or_invocation: Union["SolidDefinition", "PendingNodeInvocation"],
    context: Optional["UnboundSolidExecutionContext"],
    *args,
    **kwargs,
) -> Any:
    from dagster.core.execution.context.invocation import build_solid_context
    from dagster.core.definitions.composition import PendingNodeInvocation

    solid_def = (
        solid_def_or_invocation.node_def
        if isinstance(solid_def_or_invocation, PendingNodeInvocation)
        else solid_def_or_invocation
    )

    _check_invocation_requirements(solid_def, context)

    context = (context or build_solid_context()).bind(solid_def_or_invocation)

    input_dict = _resolve_inputs(solid_def, args, kwargs, context)

    return (
        solid_def.decorated_fn(context, **input_dict)
        if solid_def.context_arg_provided
        else solid_def.decorated_fn(**input_dict)
    )


def _check_invocation_requirements(
    solid_def: "SolidDefinition", context: Optional["UnboundSolidExecutionContext"]
) -> None:
    """Ensure that provided context fulfills requirements of solid definition.

    If no context was provided, then construct an enpty UnboundSolidExecutionContext
    """

    # Check resource requirements
    if solid_def.required_resource_keys and context is None:
        raise DagsterInvalidInvocationError(
            f'Solid "{solid_def.name}" has required resources, but no context was provided. Use the'
            "`build_solid_context` function to construct a context with the required "
            "resources."
        )

    # Check config requirements
    if not context and solid_def.config_schema.as_field().is_required:
        raise DagsterInvalidInvocationError(
            f'Solid "{solid_def.name}" has required config schema, but no context was provided. '
            "Use the `build_solid_context` function to create a context with config."
        )


def _resolve_inputs(
    solid_def: "SolidDefinition", args, kwargs, context: "BoundSolidExecutionContext"
):
    from dagster.core.execution.plan.execute_step import do_type_check

    input_defs = solid_def.input_defs

    # Fail early if too many inputs were provided.
    if len(input_defs) < len(args) + len(kwargs):
        raise DagsterInvalidInvocationError(
            f"Too many input arguments were provided for solid '{context.alias}'. This may be because "
            "an argument was provided for the context parameter, but no context parameter was defined "
            "for the solid."
        )

    input_dict = {
        input_def.name: input_val for input_val, input_def in zip(args, input_defs[: len(args)])
    }

    for input_def in input_defs[len(args) :]:
        if not input_def.has_default_value and input_def.name not in kwargs:
            raise DagsterInvalidInvocationError(
                f'No value provided for required input "{input_def.name}".'
            )

        input_dict[input_def.name] = (
            kwargs[input_def.name] if input_def.name in kwargs else input_def.default_value
        )

    # Type check inputs
    input_defs_by_name = {input_def.name: input_def for input_def in input_defs}
    for input_name, val in input_dict.items():

        input_def = input_defs_by_name[input_name]
        dagster_type = input_def.dagster_type
        type_check = do_type_check(context.for_type(dagster_type), dagster_type, val)
        if not type_check.success:
            raise DagsterTypeCheckDidNotPass(
                description=(
                    f'Type check failed for solid input "{input_def.name}" - '
                    f'expected type "{dagster_type.display_name}". '
                    f"Description: {type_check.description}."
                ),
                metadata_entries=type_check.metadata_entries,
                dagster_type=dagster_type,
            )

    return input_dict
