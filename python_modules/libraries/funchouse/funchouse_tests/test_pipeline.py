from dagster import DependencyDefinition, IOManager, ModeDefinition, execute_pipeline, io_manager
from funchouse import SourceAsset, asset, build_assets_pipeline


def test_single_asset_pipeline():
    @asset
    def asset1():
        return 1

    pipeline = build_assets_pipeline("a", [asset1])
    assert pipeline.node_defs == [asset1]
    execute_pipeline(pipeline)


def test_two_asset_pipeline():
    @asset
    def asset1():
        return 1

    @asset(inputs=[["asset1"]])
    def asset2(arg1):
        assert arg1 == 1

    pipeline = build_assets_pipeline("a", [asset1, asset2])
    assert pipeline.node_defs == [asset1, asset2]
    assert pipeline.dependencies == {
        "asset1": {},
        "asset2": {"arg1": DependencyDefinition("asset1", "result")},
    }
    execute_pipeline(pipeline)


def test_source_asset():
    @asset(inputs=[["source1"]])
    def asset1(arg1):
        assert arg1 == 5
        return 1

    class MyIOManager(IOManager):
        def handle_output(self, context, obj):
            pass

        def load_input(self, context):
            return 5

    @io_manager
    def my_io_manager(_):
        return MyIOManager()

    pipeline = build_assets_pipeline(
        "a",
        [asset1],
        source_assets=[SourceAsset(("source1",))],
        mode_defs=[ModeDefinition(resource_defs={"io_manager": my_io_manager})],
    )
    assert pipeline.node_defs == [asset1]
    execute_pipeline(pipeline)
