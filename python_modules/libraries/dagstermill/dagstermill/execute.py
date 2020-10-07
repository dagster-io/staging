import os
from collections import defaultdict, namedtuple

from dagster import (
    DagsterInstance,
    DependencyDefinition,
    ModeDefinition,
    PipelineDefinition,
    check,
    execute_pipeline,
    lambda_solid,
)
from dagster.core.code_pointer import FileCodePointer, get_python_file_from_previous_stack_frame
from dagster.core.definitions import build_reconstructable_pipeline
from dagster.serdes import DictItemsContainer
from dagster.seven import TemporaryDirectory

from .solids import DagstermillSolidDefinition


def _build_ephemeral_pipeline(
    file_code_pointer, mode_def=None, input_values=None,
):
    check.opt_inst_param(mode_def, "mode_def", ModeDefinition)
    input_values = check.inst_param(input_values, "input_values", DictItemsContainer)

    solid_callable = file_code_pointer.load_target()
    check.invariant(
        callable(solid_callable),
        "FileCodePointer did not load a callable: {type_}".format(type_=type(solid_callable)),
    )

    solid_def = solid_callable()
    check.invariant(
        isinstance(solid_def, DagstermillSolidDefinition),
        "Callable must return a Dagstermill solid: {type_}".format(type_=type(solid_def)),
    )

    solid_defs = [solid_def]

    def create_value_solid(input_name, input_value):
        @lambda_solid(name=input_name)
        def input_solid():
            return input_value

        return input_solid

    dependencies = defaultdict(dict)

    for (input_name, input_value) in input_values.dict_items:
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

    Args:
        solid_callable (Callable[[], DagstermillSolidDefinition]): Callable that returns the
            Dagstermill solid to execute.
        mode_callable (Optional[Callable[[], ModeDefinition]]): Callable that returns the ModeDefinition
            within which to execute the solid. Use this if, e.g., custom resources, loggers, or
            executors are desired.
        input_values (Optional[Dict[str, Any]]): A dict of input names to input values, used to
            pass inputs to the solid directly. You may also use the ``run_config`` to
            configure any inputs that are configurable.
        tags (Optional[Dict[str, Any]]): Arbitrary key-value pairs that will be added to pipeline
            logs.
        run_config (Optional[dict]): The environment configuration that parameterized this
            execution, as a dict.
        raise_on_error (Optional[bool]): Whether or not to raise exceptions when they occur.
            Defaults to ``True``, since this is the most useful behavior in test.

    Returns:
        SolidExecutionResult: The result of executing the solid.
    
    Examples:

        def make_hello_world_dagstermill_solid():
            return dagstermill.define_dagstermill_solid("hello_world", "hello_world.ipynb")
        
        result = execute_dagstermill_solid(make_hello_world_dagstermill_solid)
    """
    check.callable_param(solid_callable, "solid_callable")
    input_values = check.opt_dict_param(input_values, "input_values", key_type=str)

    solid_def = solid_callable()
    check.invariant(
        isinstance(solid_def, DagstermillSolidDefinition),
        "Callable solid_callable must return a Dagstermill solid, got type: {type_}".format(
            type_=type(solid_def)
        ),
    )

    check.opt_callable_param(mode_callable, "mode_callable")
    if mode_callable is not None:
        mode_def = mode_callable()
        check.invariant(
            isinstance(mode_def, ModeDefinition),
            "Callable mode_callable must return a ModeDefinition: got {type_}".format(
                type_=type(mode_def)
            ),
        )
    else:
        mode_def = None

    python_file = get_python_file_from_previous_stack_frame()

    pipeline_def = build_reconstructable_pipeline(
        "dagstermill.execute",
        "_build_ephemeral_pipeline",
        reconstructable_args=(
            FileCodePointer(python_file, solid_callable.__name__, working_directory=os.getcwd()),
        ),
        reconstructable_kwargs={
            "mode_def": mode_def,
            "input_values": DictItemsContainer(input_values.items()),
        },
    )

    with TemporaryDirectory() as tempdir:
        result = execute_pipeline(
            pipeline_def,
            run_config=run_config,
            mode=mode_def.name if mode_def else None,
            tags=tags,
            raise_on_error=raise_on_error,
            instance=DagsterInstance.local_temp(tempdir),
        )

    return result.result_for_handle(solid_def.name)
