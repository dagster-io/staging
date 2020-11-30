import os

from dagster import Field, InputDefinition, ModeDefinition, execute_pipeline, pipeline, solid
from dagster.core.storage.asset_store import build_object_manager
from dagster.core.storage.input_manager import input_manager
from dagster.core.storage.output_manager import output_manager


def load_from_path(path):
    return "__" + path


def save_to_path(_path, _obj):
    pass


@input_manager
def output_path_input_manager(context, _resource_config, _input_config):
    if "path" in context.output_config:
        path = context.output_config["path"]
    else:
        path = os.path.join(context.source_run_id, context.step_key, context.output_name)

    path = context.output_config["path"]
    return load_from_path(path)


@input_manager(input_config_schema={"path": str})
def root_path_input_manager(_context, _resource_config, input_config):
    path = input_config["path"]
    return load_from_path(path)


@output_manager(output_config_schema={"path": Field(str, is_required=False)})
def storer(context, _resource_config, output_config, obj):
    if "path" in output_config:
        path = output_config["path"]
    else:
        path = os.path.join(context.source_run_id, context.step_key, context.output_name)
    return save_to_path(path, obj)


object_manager = build_object_manager(
    input_manager_def=output_path_input_manager, output_manager_def=storer
)


@solid(input_defs=[InputDefinition("input1", manager_key="root_input_manager")])
def solid1(_, input1):
    return input1 + "1"


@solid
def solid2(_, input1):
    return input1 + "2"


@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={
                "asset_store": object_manager,
                "root_input_manager": root_path_input_manager,
            }
        )
    ]
)
def my_pipeline():
    solid2(solid1())


def test_execute_pipeline():
    execute_pipeline(
        my_pipeline, run_config={"solids": {"solid1": {"inputs": {"input1": {"path": {"abc"}}}}}}
    )


def test_solid_selection():
    execute_pipeline(
        my_pipeline,
        solid_selection=["solid2"],  # TODO: step selection
        run_config={"solids": {"solid1": {"outputs": {"result": {"path": {"123"}}}}}},
    )
