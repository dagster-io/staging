from dagster import check
from dagster.core.errors import DagsterSolidInvocationError, user_code_error_boundary


def solid_invocation_result(solid_def, *args, **kwargs):
    context, args = _get_invocation_context(solid_def, args)

    input_dict = _resolve_inputs(solid_def, args, kwargs)

    with user_code_error_boundary(
        DagsterSolidInvocationError,
        msg_fn=lambda: f'Error occurred while invoking solid "{solid_def.name}":',
    ):
        output_dict = {}
        for output in solid_def.compute_fn(context, input_dict):
            output_dict[output.output_name] = output.value

        if list(output_dict.keys()) == ["result"]:
            return output_dict["result"]

        return output_dict


def _get_invocation_context(solid_def, args):
    """Retrieves context from args if exists, and splits it from the rest of the args."""

    from dagster.core.execution.context.compute import SolidExecutionContext

    if len(args) > 0 and isinstance(args[0], SolidExecutionContext):
        return args[0], args[1:]
    else:
        _check_is_context_required(solid_def)
        return None, args


def _check_is_context_required(solid_def):
    """Ensure that additional environmental information is not required in order to execute."""
    # TODO: Add to error message describing more clearly how to use invokable.
    check.invariant(
        not solid_def.required_resource_keys,
        f'Solid "{solid_def.name}" has required resources, but no resources have been provided. '
        "Resources can be provided using the `invokable` method on the solid.",
    )

    check.invariant(
        not solid_def.config_schema.as_field().is_required,
        f'Solid "{solid_def.name}" has required config schema, but no config has been provided. '
        "Config can be provided using the `invokable` method on the solid.",
    )


def _resolve_inputs(solid_def, args, kwargs):
    input_defs = solid_def.input_defs

    # Ensure that the expected number of inputs were provided.
    num_provided_inputs = len(args) + len(kwargs)
    check.invariant(
        num_provided_inputs == len(input_defs),
        f"Solid \"{solid_def.name}\" expected {len(input_defs)} inputs, but {num_provided_inputs} "
        "were provided.",
    )

    input_dict = {
        input_def.name: input_val for input_val, input_def in zip(args, input_defs[: len(args)])
    }

    for (key, val), input_def in zip(kwargs.items(), input_defs[len(args) :]):
        # If someone provided a kwarg, make sure that it corresponds to the correct input.
        if key:
            check.invariant(
                key == input_def.name,
                f'Keyword argument "{key}" provided at position of argument '
                f'"{input_def.name}".',
            )
        input_dict[input_def.name] = val
    return input_dict
