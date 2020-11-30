from dagster import (
    InputDefinition,
    ModeDefinition,
    PythonObjectDagsterType,
    execute_pipeline,
    pipeline,
    solid,
)
from dagster.core.storage.input_manager import input_manager


def test_validate_inputs():
    @input_manager
    def my_loader(_, _resource_config, _input_config):
        return 5

    @solid(
        input_defs=[
            InputDefinition(
                "input1", dagster_type=PythonObjectDagsterType(int), manager_key="my_loader"
            )
        ]
    )
    def my_solid(_, input1):
        return input1

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"my_loader": my_loader})])
    def my_pipeline():
        my_solid()

    execute_pipeline(my_pipeline)
