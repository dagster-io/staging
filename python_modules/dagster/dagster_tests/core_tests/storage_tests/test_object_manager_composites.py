from dagster import (
    ModeDefinition,
    OutputDefinition,
    composite_solid,
    execute_pipeline,
    pipeline,
    solid,
)
from dagster.core.storage.object_manager import ObjectManager, object_manager


def named_object_manager(storage_dict, name):
    @object_manager
    def my_object_manager(_):
        class MyObjectManager(ObjectManager):
            def handle_output(self, context, obj):
                storage_dict[tuple(context.get_run_scoped_output_identifier())] = {
                    "value": obj,
                    "output_manager_name": name,
                }

            def load_input(self, context):
                result = storage_dict[
                    tuple(context.upstream_output.get_run_scoped_output_identifier())
                ]
                return {**result, "input_manager_name": name}

        return MyObjectManager()

    return my_object_manager


def test_composite_solid_output():
    """Is the manager_key on the inner or outer solid used for storing outputs?"""

    @solid(output_defs=[OutputDefinition(manager_key="inner_manager")])
    def my_solid(_):
        return 5

    @composite_solid(output_defs=[OutputDefinition(manager_key="outer_manager")])
    def my_composite():
        return my_solid()

    storage_dict = {}

    @pipeline(
        mode_defs=[
            ModeDefinition(
                resource_defs={
                    "inner_manager": named_object_manager(storage_dict, "inner"),
                    "outer_manager": named_object_manager(storage_dict, "outer"),
                }
            )
        ]
    )
    def my_pipeline():
        my_composite()

    result = execute_pipeline(my_pipeline)
    assert result.output_for_solid("my_composite") == {
        "value": 5,
        "output_manager_name": "inner",
        "input_manager_name": "inner",
    }


def test_composite_solid_upstream_output():
    """Is the manager_key on the inner or outer upstream solid used for loading downstream inputs?
    """

    @solid(output_defs=[OutputDefinition(manager_key="inner_manager")])
    def my_solid(_):
        return 5

    @composite_solid(output_defs=[OutputDefinition(manager_key="outer_manager")])
    def my_composite():
        return my_solid()

    @solid
    def downstream_solid(_, input1):
        assert input1 == {
            "value": 5,
            "output_manager_name": "inner",
            "input_manager_name": "inner",
        }

    storage_dict = {}

    @pipeline(
        mode_defs=[
            ModeDefinition(
                resource_defs={
                    "inner_manager": named_object_manager(storage_dict, "inner"),
                    "outer_manager": named_object_manager(storage_dict, "outer"),
                }
            )
        ]
    )
    def my_pipeline():
        downstream_solid(my_composite())

    result = execute_pipeline(my_pipeline)
    assert result.success
