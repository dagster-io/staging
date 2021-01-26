from dagster import (
    AssetKey,
    EventMetadataEntry,
    ModeDefinition,
    OutputContext,
    OutputDefinition,
    execute_pipeline,
    io_manager,
    pipeline,
    solid,
)
from dagster.core.storage.asset_io_manager import AssetIOManager
from dagster.core.storage.mem_io_manager import InMemoryIOManager


def test_asset_io_manager():
    metadata_entry = EventMetadataEntry.text("fdsjk", label="my_label")

    class MemAssetIOManager(InMemoryIOManager, AssetIOManager):
        def get_asset_key(self, context: OutputContext) -> AssetKey:
            return AssetKey([context.metadata["dir"], context.name])

        def handle_output(self, context: OutputContext, obj):
            yield metadata_entry
            super(MemAssetIOManager, self).handle_output(context, obj)

    @io_manager
    def my_asset_io_manager(_):
        return MemAssetIOManager()

    @solid(output_defs=[OutputDefinition(name="output1", metadata={"dir": "a"})])
    def solid1(_):
        return 5

    @solid(output_defs=[OutputDefinition(name="output2", metadata={"dir": "b"})])
    def solid2(_, _input1):
        return 7

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"io_manager": my_asset_io_manager})])
    def my_pipeline():
        solid2(solid1())

    result = execute_pipeline(my_pipeline)
    events = result.step_event_list
    materializations = [
        event for event in events if event.event_type_value == "STEP_MATERIALIZATION"
    ]
    assert len(materializations) == 2
    materialization1 = materializations[0].event_specific_data.materialization
    assert materialization1.asset_key == AssetKey(["a", "output1"])
    assert materialization1.parent_asset_keys == []

    materialization2 = materializations[1].event_specific_data.materialization
    assert materialization2.asset_key == AssetKey(["b", "output2"])
    assert materialization2.parent_asset_keys == [AssetKey(["a", "output1"])]

    assert materialization1.metadata_entries == [metadata_entry]
