import pytest
from dagster import (
    DagsterInvalidDefinitionError,
    InputDefinition,
    ModeDefinition,
    OutputDefinition,
    execute_pipeline,
    pipeline,
    solid,
)
from dagster.core.storage.input_manager import input_manager
from dagster.core.storage.output_manager import output_manager


def test_output_manager():
    adict = {}

    @output_manager
    def my_output_manager(_context, _resource_config, _output_config, obj):
        adict["result"] = obj

    @solid(output_defs=[OutputDefinition(manager_key="my_output_manager")])
    def my_solid(_):
        return 5

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"my_output_manager": my_output_manager})])
    def my_pipeline():
        my_solid()

    execute_pipeline(my_pipeline)

    assert adict["result"] == 5


def test_output_manager_no_input_manager():
    adict = {}

    @output_manager
    def my_output_manager(_context, _resource_config, _output_config, obj):
        adict["result"] = obj

    @solid(output_defs=[OutputDefinition(manager_key="my_output_manager")])
    def my_solid(_):
        return 5

    @solid
    def my_downstream_solid(_, input1):
        return input1 + 1

    with pytest.raises(DagsterInvalidDefinitionError):

        @pipeline(
            mode_defs=[ModeDefinition(resource_defs={"my_output_manager": my_output_manager})]
        )
        def my_pipeline():
            my_downstream_solid(my_solid())

        assert my_pipeline


def test_separate_output_manager_input_manager():
    adict = {}

    @output_manager
    def my_output_manager(_context, _resource_config, _output_config, obj):
        adict["result"] = obj

    @input_manager
    def my_input_manager(_context, _resource_config, _output_config):
        return adict["result"]

    @solid(output_defs=[OutputDefinition(manager_key="my_output_manager")])
    def my_solid(_):
        return 5

    @solid(input_defs=[InputDefinition("input1", manager_key="my_input_manager")])
    def my_downstream_solid(_, input1):
        return input1 + 1

    @pipeline(
        mode_defs=[
            ModeDefinition(
                resource_defs={
                    "my_input_manager": my_input_manager,
                    "my_output_manager": my_output_manager,
                }
            )
        ]
    )
    def my_pipeline():
        my_downstream_solid(my_solid())

    execute_pipeline(my_pipeline)

    assert adict["result"] == 5
