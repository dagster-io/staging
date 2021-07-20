from dagster import DependencyDefinition, IOManager, io_manager
from dagster.assets import SourceAsset, asset, build_assets_job


def test_single_asset_pipeline():
    @asset
    def asset1():
        return 1

    job = build_assets_job("a", [asset1])
    assert job.graph.node_defs == [asset1]
    assert job.execute_in_process().success


def test_two_asset_pipeline():
    @asset
    def asset1():
        return 1

    @asset(inputs=[["asset1"]])
    def asset2(arg1):
        assert arg1 == 1

    job = build_assets_job("a", [asset1, asset2])
    assert job.graph.node_defs == [asset1, asset2]
    assert job.dependencies == {
        "asset1": {},
        "asset2": {"arg1": DependencyDefinition("asset1", "result")},
    }
    assert job.execute_in_process().success


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

    job = build_assets_job(
        "a",
        [asset1],
        source_assets=[SourceAsset(("source1",))],
        resource_defs={"io_manager": my_io_manager},
    )
    assert job.graph.node_defs == [asset1]
    assert job.execute_in_process().success
