from dagster import (
    AssetKey,
    EventMetadataEntry,
    ModeDefinition,
    Output,
    OutputContext,
    OutputDefinition,
    execute_pipeline,
    io_manager,
    pipeline,
    solid,
)
from dagster.core.definitions import AssetMaterialization, AssetOutput


def test_asset_io_manager():
    @solid(output_defs=[OutputDefinition(name="output1", metadata={"dir": "a"})])
    def solid1(_):
        yield AssetOutput(
            5,
            "output1",
            asset_materializations=[
                AssetMaterialization(AssetKey(["table1", "partition1"])),
                AssetMaterialization(AssetKey(["table1", "partition2"])),
            ],
        )

    @solid(output_defs=[OutputDefinition(name="output2", metadata={"dir": "b"})])
    def solid2(_, _input1):
        yield AssetOutput(
            7,
            "output2",
            asset_materializations=[AssetMaterialization(AssetKey(["table2", "partition1"])),],
        )

    @pipeline
    def my_pipeline():
        solid2(solid1())

    result = execute_pipeline(my_pipeline)
    events = result.step_event_list
    materializations = [
        event for event in events if event.event_type_value == "STEP_MATERIALIZATION"
    ]
    assert len(materializations) == 3

    event_data1 = materializations[0].event_specific_data
    assert event_data1.materialization.asset_key == AssetKey(["table1", "partition1"])
    assert event_data1.parent_asset_keys == []

    event_data2 = materializations[1].event_specific_data
    assert event_data2.materialization.asset_key == AssetKey(["table1", "partition2"])
    assert event_data2.parent_asset_keys == []

    event_data3 = materializations[2].event_specific_data
    assert event_data3.materialization.asset_key == AssetKey(["table2", "partition1"])
    assert event_data3.parent_asset_keys == [
        AssetKey(["table1", "partition1"]),
        AssetKey(["table1", "partition2"]),
    ]
