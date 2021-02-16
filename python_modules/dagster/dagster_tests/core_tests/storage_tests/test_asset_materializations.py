from dagster import (
    AssetKey,
    ModeDefinition,
    Output,
    OutputDefinition,
    execute_pipeline,
    io_manager,
    pipeline,
    solid,
)
from dagster.core.definitions.events import EventMetadataEntry, PartitionSpecificMetadataEntry
from dagster.core.storage.io_manager import IOManager


def n_asset_keys(path, n):
    return [AssetKey(path, str(i)) for i in range(n)]


def test_output_definition_single_partition_materialization():

    entry1 = EventMetadataEntry.int(123, "nrows")
    entry2 = EventMetadataEntry.float(3.21, "some value")

    @solid(
        output_defs=[OutputDefinition(name="output1", asset_keys_fn=lambda _: [AssetKey("table1")])]
    )
    def solid1(_):
        return Output(None, "output1", metadata_entries=[entry1])

    @solid(
        output_defs=[OutputDefinition(name="output2", asset_keys_fn=lambda _: [AssetKey("table2")])]
    )
    def solid2(_, _input1):
        yield Output(
            7,
            "output2",
            metadata_entries=[entry2],
        )

    @pipeline
    def my_pipeline():
        solid2(solid1())

    result = execute_pipeline(my_pipeline)
    events = result.step_event_list
    materializations = [
        event for event in events if event.event_type_value == "STEP_MATERIALIZATION"
    ]
    assert len(materializations) == 2

    event_data1 = materializations[0].event_specific_data
    assert event_data1.materialization.asset_key == AssetKey(["table1"])
    assert event_data1.materialization.metadata_entries == [entry1]
    assert event_data1.parent_asset_keys == []

    event_data2 = materializations[1].event_specific_data
    assert event_data2.materialization.asset_key == AssetKey(["table2"])
    assert event_data2.materialization.metadata_entries == [entry2]
    assert event_data2.parent_asset_keys == [AssetKey(["table1"])]


def test_output_definition_multiple_partition_materialization():

    entry1 = EventMetadataEntry.int(123, "nrows")
    entry2 = EventMetadataEntry.float(3.21, "some value")

    partition_entries = [EventMetadataEntry.int(123 * i * i, "partition count") for i in range(3)]

    @solid(
        output_defs=[
            OutputDefinition(name="output1", asset_keys_fn=lambda _: n_asset_keys("table1", 3))
        ]
    )
    def solid1(_):
        return Output(
            None,
            "output1",
            metadata_entries=[
                entry1,
                *[
                    PartitionSpecificMetadataEntry(str(i), entry)
                    for i, entry in enumerate(partition_entries)
                ],
            ],
        )

    @solid(
        output_defs=[OutputDefinition(name="output2", asset_keys_fn=lambda _: [AssetKey("table2")])]
    )
    def solid2(_, _input1):
        yield Output(
            7,
            "output2",
            metadata_entries=[entry2],
        )

    @pipeline
    def my_pipeline():
        solid2(solid1())

    result = execute_pipeline(my_pipeline)
    events = result.step_event_list
    materializations = [
        event for event in events if event.event_type_value == "STEP_MATERIALIZATION"
    ]
    assert len(materializations) == 4

    for i in range(3):
        event_data = materializations[i].event_specific_data
        assert event_data.materialization.asset_key == AssetKey(["table1"], str(i))
        assert event_data.materialization.metadata_entries == [entry1, partition_entries[i]]
        assert event_data.parent_asset_keys == []

    event_data2 = materializations[-1].event_specific_data
    assert event_data2.materialization.asset_key == AssetKey(["table2"])
    assert event_data2.materialization.metadata_entries == [entry2]
    assert event_data2.parent_asset_keys == n_asset_keys("table1", 3)


def test_io_manager_single_partition_materialization():

    entry1 = EventMetadataEntry.int(123, "nrows")
    entry2 = EventMetadataEntry.float(3.21, "some value")

    class MyIOManager(IOManager):
        def handle_output(self, context, obj):
            # store asset
            yield entry1

        def load_input(self, context):
            return None

        def get_output_asset_keys(self, context):
            return [AssetKey([context.step_key])]

    @io_manager
    def my_io_manager(_):
        return MyIOManager()

    @solid(output_defs=[OutputDefinition(name="output1")])
    def solid1(_):
        return Output(None, "output1")

    @solid(output_defs=[OutputDefinition(name="output2")])
    def solid2(_, _input1):
        yield Output(
            7,
            "output2",
            metadata_entries=[entry2],
        )

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"io_manager": my_io_manager})])
    def my_pipeline():
        solid2(solid1())

    result = execute_pipeline(my_pipeline)
    events = result.step_event_list
    materializations = [
        event for event in events if event.event_type_value == "STEP_MATERIALIZATION"
    ]
    assert len(materializations) == 2

    event_data1 = materializations[0].event_specific_data
    assert event_data1.materialization.asset_key == AssetKey(["solid1"])
    assert event_data1.materialization.metadata_entries == [entry1]
    assert event_data1.parent_asset_keys == []

    event_data2 = materializations[1].event_specific_data
    assert event_data2.materialization.asset_key == AssetKey(["solid2"])
    assert set(event_data2.materialization.metadata_entries) == set([entry1, entry2])
    assert event_data2.parent_asset_keys == [AssetKey(["solid1"])]
