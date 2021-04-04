import inspect

from dagster.check import CheckError
from dagster.core.errors import DagsterInvalidDefinitionError
from dagster.seven import funcsigs, is_module_available

from .input import InputDefinition, _NoValueSentinel
from .output import OutputDefinition


def _infer_input_description_from_docstring(fn):
    if not is_module_available("docstring_parser"):
        return {}

    from docstring_parser import parse

    docstring = parse(fn.__doc__)
    return {p.arg_name: p.description for p in docstring.params}


def _infer_output_description_from_docstring(fn):
    if not is_module_available("docstring_parser"):
        return
    from docstring_parser import parse

    docstring = parse(fn.__doc__)
    if docstring.returns is None:
        return

    return docstring.returns.description


def infer_output_definitions(decorator_name, solid_name, fn):
    signature = funcsigs.signature(fn)
    try:
        description = _infer_output_description_from_docstring(fn)
        return [
            OutputDefinition()
            if signature.return_annotation is funcsigs.Signature.empty
            else OutputDefinition(signature.return_annotation, description=description)
        ]

    except CheckError as type_error:
        raise DagsterInvalidDefinitionError(
            "Error inferring Dagster type for return type "
            f'"{signature.return_annotation}" from {decorator_name} "{solid_name}". '
            f"Correct the issue or explicitly pass definitions to {decorator_name}."
        ) from type_error


def has_explicit_return_type(fn):
    signature = funcsigs.signature(fn)
    return not signature.return_annotation is funcsigs.Signature.empty


def _input_param_type(type_annotation):
    if type_annotation is not inspect.Parameter.empty:
        return type_annotation
    return None


def infer_input_definitions_for_lambda_solid(solid_name, fn):
    signature = funcsigs.signature(fn)
    params = list(signature.parameters.values())
    descriptions = _infer_input_description_from_docstring(fn)
    defs = _infer_inputs_from_params(params, "@lambda_solid", solid_name, descriptions=descriptions)
    return defs


def _infer_inputs_from_params(
    params, decorator_name, solid_name, descriptions=None, condensed_input_defs=None
):
    descriptions = descriptions or {}
    condensed_input_defs = condensed_input_defs or {}
    input_defs = []

    seen_input_names = set()

    from dagster.core.definitions.decorators.solid import (
        input_def_ish_to_input_definition,
        condensed_def_to_def,
    )

    for param in params:
        try:
            default_value = (
                param.default if param.default is not funcsigs.Parameter.empty else _NoValueSentinel
            )
            if param.name in condensed_input_defs:
                condensed_input_def = condensed_input_defs[param.name]
                input_def = condensed_def_to_def(
                    param.name,
                    condensed_input_def,
                    type_override=_input_param_type(param.annotation),
                )
            else:
                input_def = InputDefinition(
                    param.name,
                    _input_param_type(param.annotation),
                    default_value=default_value,
                    description=descriptions.get(param.name),
                )

            input_defs.append(input_def)

            seen_input_names.add(input_def.name)
        except CheckError as type_error:
            raise DagsterInvalidDefinitionError(
                f"Error inferring Dagster type for input name {param.name} typed as "
                f'"{param.annotation}" from {decorator_name} "{solid_name}". '
                "Correct the issue or explicitly pass definitions to {decorator_name}."
            ) from type_error

    for name, condensed_input_def in condensed_input_defs.items():
        if name in seen_input_names:
            continue

        # else we have a nothing dependency

        input_def = input_def_ish_to_input_definition(name, condensed_input_def)
        input_defs.append(input_def)

    return input_defs


def infer_input_definitions_for_graph(decorator_name, solid_name, fn):
    signature = funcsigs.signature(fn)
    params = list(signature.parameters.values())
    descriptions = _infer_input_description_from_docstring(fn)
    defs = _infer_inputs_from_params(params, decorator_name, solid_name, descriptions=descriptions)
    return defs


def infer_input_definitions_for_solid(solid_name, fn, condensed_input_defs):
    signature = funcsigs.signature(fn)
    params = list(signature.parameters.values())
    descriptions = _infer_input_description_from_docstring(fn)
    defs = _infer_inputs_from_params(
        params[1:],
        "@solid",
        solid_name,
        descriptions=descriptions,
        condensed_input_defs=condensed_input_defs,
    )
    return defs
