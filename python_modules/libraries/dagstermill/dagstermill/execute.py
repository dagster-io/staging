import json
import os
import tempfile
from collections import defaultdict

from dagster import (
    DagsterInstance,
    DependencyDefinition,
    ModeDefinition,
    PipelineDefinition,
    check,
    execute_pipeline,
    lambda_solid,
)
from dagster.core.code_pointer import FileCodePointer, get_python_file_from_target
from dagster.core.definitions import build_reconstructable_pipeline
from dagster.core.definitions.i_solid_definition import NodeDefinition


def _build_ephemeral_pipeline(solid_code_pointer, mode_code_pointer=None, input_values_str=None):
    input_values_str = check.opt_str_param(input_values_str, "input_values_str")
    input_values = check.opt_dict_param(json.loads(input_values_str), "input_values", key_type=str)

    # handle out of process user code threading
    solid_callable = solid_code_pointer.load_target()
    solid_def = check.inst_param(solid_callable(), "solid_def", NodeDefinition)
    if mode_code_pointer:
        mode_callable = mode_code_pointer.load_target()
        mode_def = check.opt_inst_param(mode_callable(), "mode_def", ModeDefinition)
    else:
        mode_def = None

    solid_defs = [solid_def]

    def create_value_solid(input_name, input_value):
        @lambda_solid(name=input_name)
        def input_solid():
            return input_value

        return input_solid

    dependencies = defaultdict(dict)

    for (input_name, input_value) in input_values.items():
        dependencies[solid_def.name][input_name] = DependencyDefinition(input_name)
        solid_defs.append(create_value_solid(input_name, input_value))

    return PipelineDefinition(
        name="ephemeral_{}_solid_pipeline".format(solid_def.name),
        solid_defs=solid_defs,
        dependencies=dependencies,
        mode_defs=[mode_def] if mode_def else None,
    )


def execute_dagstermill_solid(
    solid_callable,
    mode_callable=None,
    input_values=None,
    tags=None,
    run_config=None,
    raise_on_error=True,
):
    """Execute a single dagstermill solid in an ephemeral pipeline.

    Specialized function for dagstermill solids, which cannot be executed using
    `dagster.execute_solid`. Instead of passing the solid definition, you must pass a function of
    no arguments that returns the solid definition.

    Intended to support unit tests. Input values may be passed directly, and no pipeline need be
    specified -- an ephemeral pipeline will be constructed.

    ...
    """
    input_values = check.opt_dict_param(input_values, "input_values", key_type=str)

    check.callable_param(solid_callable, "solid_callable")
    solid_def = check.inst_param(solid_callable(), "solid_def", NodeDefinition)

    check.opt_callable_param(mode_callable, "mode_callable")
    if mode_callable:
        mode_def = check.opt_inst_param(mode_callable(), "mode_def", ModeDefinition)
    else:
        mode_def = None

    reconstructable_pipeline = build_reconstructable_pipeline(
        "dagstermill.execute",
        "_build_ephemeral_pipeline",
        reconstructable_args=(
            FileCodePointer(
                get_python_file_from_target(solid_callable),
                solid_callable.__name__,
                working_directory=os.getcwd(),
            ),
        ),
        reconstructable_kwargs={
            "mode_code_pointer": FileCodePointer(
                get_python_file_from_target(mode_callable),
                mode_callable.__name__,
                working_directory=os.getcwd(),
            )
            if mode_callable
            else None,
            "input_values_str": json.dumps(input_values),
        },
    )

    solid_defs = [solid_def]

    def create_value_solid(input_name, input_value):
        @lambda_solid(name=input_name)
        def input_solid():
            return input_value

        return input_solid

    dependencies = defaultdict(dict)

    for input_name, input_value in input_values.items():
        dependencies[solid_def.name][input_name] = DependencyDefinition(input_name)
        solid_defs.append(create_value_solid(input_name, input_value))

    with tempfile.TemporaryDirectory() as temp_dir:
        result = execute_pipeline(
            reconstructable_pipeline,
            run_config=run_config,
            mode=mode_def.name if mode_def else None,
            tags=tags,
            raise_on_error=raise_on_error,
            instance=DagsterInstance.local_temp(temp_dir),
        )

    return result.result_for_handle(solid_def.name)
