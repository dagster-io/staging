from dagster import AssetKey, DependencyDefinition, IOManager, io_manager, op
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

    @asset
    def asset2(asset1):
        assert asset1 == 1

    job = build_assets_job("a", [asset1, asset2])
    assert job.graph.node_defs == [asset1, asset2]
    assert job.dependencies == {
        "asset1": {},
        "asset2": {"asset1": DependencyDefinition("asset1", "result")},
    }
    assert job.execute_in_process().success


def test_source_asset():
    @asset
    def asset1(source1):
        assert source1 == 5
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
        source_assets=[SourceAsset(AssetKey("source1"))],
        resource_defs={"io_manager": my_io_manager},
    )
    assert job.graph.node_defs == [asset1]
    assert job.execute_in_process().success


def test_after_nodes():
    @asset
    def asset1():
        return 1

    @op
    def after_op(arg1):
        assert arg1 == 1

    job = build_assets_job(
        "a", assets=[asset1], nodes_after=[(after_op, {"arg1": AssetKey("asset1")})]
    )
    assert job.graph.node_defs == [asset1, after_op]
    assert job.dependencies == {
        "after_op": {"arg1": DependencyDefinition("asset1", "result")},
        "asset1": {},
    }

    assert job.execute_in_process().success
