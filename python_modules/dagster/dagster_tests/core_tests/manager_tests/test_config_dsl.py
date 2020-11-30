"""

I have provided a DSL for less technical users that utilizes the config system.
They are able to change the behavior of the pipeline without touching python.

Although this example is just in terms of numbers one can instead imagine a
system where you enable users to specify data frame filtering operations
via the config system. (There are users that already do this)
"""


from dagster import (
    AssetStore,
    Field,
    ModeDefinition,
    Selector,
    execute_pipeline,
    execute_solid,
    pipeline,
    solid,
)
from dagster.core.storage.asset_store import asset_store


@solid(config_schema=[Selector({"add": int, "subtract": int})])
def unary_operation(context, num: int) -> int:

    for operation in context.solid_config:
        op, value = list(operation.items())[0]

        if op == "add":
            num = num + value
        elif op == "subtract":
            num = num - value
        else:
            raise Exception(f"Unsupported op {op}")

    return num


@solid(config_schema=str)
def binary_operation(context, left: int, right: int) -> int:
    op = context.solid_config
    if op == "add":
        return left + right
    elif op == "subtract":
        return left - right
    else:
        raise Exception(f"Unsupported op {op}")


def test_single_unary_operation():
    result = execute_solid(
        unary_operation,
        run_config={"solids": {"unary_operation": {"inputs": {"num": 1}, "config": [{"add": 2}]}}},
    )

    assert result.output_value() == 3


def test_multiple_unary_operation():
    result = execute_solid(
        unary_operation,
        run_config={
            "solids": {
                "unary_operation": {"inputs": {"num": 1}, "config": [{"add": 5}, {"subtract": 2}]}
            }
        },
    )

    assert result.output_value() == 4


def test_little_pipeline():
    @pipeline
    def little_tree():
        binary_operation(unary_operation.alias("alice_op")(), unary_operation.alias("bob_op")())

    result = execute_pipeline(
        little_tree,
        run_config={
            "solids": {
                "alice_op": {"inputs": {"num": 5}, "config": [{"add": 3}, {"subtract": 1}]},
                "bob_op": {"inputs": {"num": 7}, "config": [{"add": 1}]},
                "binary_operation": {"config": "add"},
            }
        },
    )

    assert result.success

    assert result.output_for_solid("binary_operation") == 15


# Now the scenario that impacts loaders etc is that imagine instead of
# having this on the config of the solid, that the solids are fixed
# operations and instead the user wants to be able to inject arbitrary
# filtering on inputs and outputs


def test_filtering_dsl_inputs():
    class InputMultiplyObjectManager(AssetStore):
        def __init__(self):
            self.values = {}

        def set_asset(self, context, obj):
            keys = tuple(context.get_run_scoped_output_identifier())
            self.values[keys] = obj

        def get_asset(self, context):
            keys = tuple(context.get_run_scoped_output_identifier())
            return self.values[keys] * context.input_config.get("multiplier", 1)

    @asset_store(input_config_schema={"multiplier": Field(int, is_required=False)})
    def input_multiply_object_manager(_):
        return InputMultiplyObjectManager()

    @solid
    def preexisting_with_optional_input_config(_, left, right):
        # shut lint up
        assert left
        assert right
        # imagine some computation crafted by an engineer here
        return left

    @pipeline(
        mode_defs=[ModeDefinition(resource_defs={"asset_store": input_multiply_object_manager})]
    )
    def todo():
        preexisting_with_optional_input_config(
            unary_operation.alias("alice_op")(), unary_operation.alias("bob_op")()
        )

    assert todo
    execute_pipeline(
        todo,
        run_config={
            "solids": {
                "alice_op": {"inputs": {"num": 5}, "config": [{"add": 3}, {"subtract": 1}]},
                "bob_op": {"inputs": {"num": 7}, "config": [{"add": 3}, {"subtract": 1}]},
                # here we hypothesize a scenario where the user wants to be able
                # to allow users to inject pre-defined filters (e.g filter all rows
                # where column is null) on inputs so that less technical users
                # can debug and fix things while modifying config only
                "preexisting_with_optional_input_config": {"inputs": {"left": {"multiplier": 5}}},
            }
        },
    )
