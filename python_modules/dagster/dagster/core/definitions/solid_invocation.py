import inspect

from dagster import check
from dagster.core.errors import DagsterSolidInvocationError, user_code_error_boundary


def solid_invocation_result(solid_def, *args, **kwargs):

    _check_context_requirements_fulfilled(solid_def)

    input_dict = _resolve_inputs(solid_def, args, kwargs)

    with user_code_error_boundary(
        DagsterSolidInvocationError,
        msg_fn=lambda: f'Error occurred while invoking solid "{solid_def.name}":',
    ):
        output_dict = _execute_and_retrieve_outputs(solid_def, input_dict)

        if list(output_dict.keys()) == ["result"]:
            return output_dict["result"]

        return output_dict


def _check_context_requirements_fulfilled(solid_def):
    """Ensure that additional environmental information is not required in order to execute."""
    # TODO: Add to error message describing more clearly how to use invokable.
    check.invariant(
        not solid_def.required_resource_keys,
        f'Solid "{solid_def.name}" has required resources. '
        "Directly invoking solids that require resources is not yet supported.",
    )

    check.invariant(
        not solid_def.config_schema.as_field().is_required,
        f'Solid "{solid_def.name}" has required config schema, but no config has been provided. '
        "Config can be provided using the `configured` API: "
        "https://docs.dagster.io/concepts/configuration/configured#configured-api",
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


def _execute_and_retrieve_outputs(solid_def, input_dict):
    from dagster.core.execution.plan.compute import gen_from_async_gen
    from dagster.core.execution.context.compute import DirectSolidExecutionContext

    output_dict = {}
    config_evr = solid_def.apply_config_mapping({"config": {}})

    compute_iterator = solid_def.compute_fn(
        DirectSolidExecutionContext(
            solid_config=config_evr.value.get("config", {}),
        ),
        input_dict,
    )

    if inspect.isasyncgen(compute_iterator):
        compute_iterator = gen_from_async_gen(compute_iterator)

    for output in compute_iterator:
        output_dict[output.output_name] = output.value

    return output_dict
