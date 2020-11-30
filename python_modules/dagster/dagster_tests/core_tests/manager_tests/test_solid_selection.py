import os

from dagster import InputDefinition, ModeDefinition, execute_pipeline, pipeline, solid
from dagster.core.storage.asset_store import build_object_manager
from dagster.core.storage.input_manager import build_composite_input_manager, input_manager
from dagster.core.storage.output_manager import output_manager


def load_from_path(path):
    return "__" + path


def path_from_output(context):
    return os.path.join(context.source_run_id, context.step_key, context.output_name)


def save_to_path(_path, _obj):
    pass


@input_manager(input_config_schema=str)
def path_input_manager(_context, _resource_config, input_config):
    return load_from_path(input_config)


@input_manager
def upstream_input_manager(context, _resource_config, _input_config):
    return load_from_path(path_from_output(context))


path_or_upstream_input_manager = build_composite_input_manager(
    {"path": path_input_manager, "upstream": upstream_input_manager}, default="upstream"
)


@output_manager
def storer(context, _resource_config, _output_config, obj):
    path = path_from_output(context)
    return save_to_path(path, obj)


object_manager = build_object_manager(
    input_manager_def=path_or_upstream_input_manager, output_manager_def=storer
)


@solid(input_defs=[InputDefinition("input1", manager_key="my_input_manager")])
def solid1(_, input1):
    return input1 + "1"


@solid(input_defs=[InputDefinition("input1", manager_key="my_input_manager")])
def solid2(_, input1):
    return input1 + "2"


@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={
                "asset_store": object_manager,
                "my_input_manager": path_or_upstream_input_manager,
            }
        )
    ]
)
def my_pipeline():
    solid2(solid1())


def test_execute_pipeline():
    execute_pipeline(
        my_pipeline, run_config={"solids": {"solid1": {"inputs": {"input1": {"path": "abc"}}}}},
    )


def test_solid_selection():
    execute_pipeline(
        my_pipeline,
        solid_selection=["solid2"],
        run_config={"solids": {"solid2": {"inputs": {"input1": {"path": "123"}}}}},
    )
