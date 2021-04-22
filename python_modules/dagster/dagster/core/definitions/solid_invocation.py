from dagster import check
from dagster.core.errors import DagsterSolidInvocationError, user_code_error_boundary


def solid_invocation_result(solid_def, *args, **kwargs):
    from dagster.core.execution.context.compute import SolidExecutionContext

    _check_context_requirements_fulfilled(solid_def)

    input_dict = _resolve_inputs(solid_def, args, kwargs)

    with user_code_error_boundary(
        DagsterSolidInvocationError,
        msg_fn=lambda: f'Error occurred while invoking solid "{solid_def.name}":',
    ):
        output_dict = {}
        config_evr = solid_def.apply_config_mapping({"config": {}})
        for output in solid_def.compute_fn(
            SolidExecutionContext(
                run_id="EPHEMERAL",
                solid_config=config_evr.value.get("config", {}),
                resources=None,
                log_manager=None,
                pipeline_run=None,
                instance=None,
                step_launcher=None,
                pipeline_def=None,
                mode_def=None,
                solid_handle=None,
                step_execution_context=None,
            ),
            input_dict,
        ):
            output_dict[output.output_name] = output.value

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

    # Ensure that the expected number of inputs were provided.
    num_provided_inputs = len(args) + len(kwargs)
    check.invariant(
        num_provided_inputs == len(input_defs),
        f'Solid "{solid_def.name}" expected {len(input_defs)} inputs, but {num_provided_inputs} '
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
