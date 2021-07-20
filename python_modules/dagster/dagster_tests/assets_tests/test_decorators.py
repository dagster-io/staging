from dagster import SolidDefinition
from dagster.assets import asset


def test_asset_no_decorator_args():
    @asset
    def my_asset():
        return 1

    assert isinstance(my_asset, SolidDefinition)
    assert len(my_asset.output_defs) == 1
    assert len(my_asset.input_defs) == 0


def test_asset_with_inputs():
    @asset
    def my_asset(arg1):
        return arg1

    assert isinstance(my_asset, SolidDefinition)
    assert len(my_asset.output_defs) == 1
    assert len(my_asset.input_defs) == 1
    assert my_asset.input_defs[0].metadata["logical_asset_name"] == "arg1"
    assert my_asset.input_defs[0].metadata["logical_asset_namespace"] is None


def test_asset_with_inputs_and_namespace():
    @asset(namespace="my_namespace")
    def my_asset(arg1):
        return arg1

    assert isinstance(my_asset, SolidDefinition)
    assert len(my_asset.output_defs) == 1
    assert len(my_asset.input_defs) == 1
    assert my_asset.input_defs[0].metadata["logical_asset_name"] == "arg1"
    assert my_asset.input_defs[0].metadata["logical_asset_namespace"] == "my_namespace"


def test_asset_with_context_arg():
    @asset
    def my_asset(context):
        context.log("hello")

    assert isinstance(my_asset, SolidDefinition)
    assert len(my_asset.input_defs) == 0
