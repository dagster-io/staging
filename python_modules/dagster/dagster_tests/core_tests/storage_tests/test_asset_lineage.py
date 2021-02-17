import pytest
from dagster import (
    AssetKey,
    InputDefinition,
    ModeDefinition,
    Output,
    OutputDefinition,
    execute_pipeline,
    io_manager,
    pipeline,
    solid,
)
from dagster.core.definitions.events import (
    AssetPartitions,
    EventMetadataEntry,
    PartitionSpecificMetadataEntry,
)
from dagster.core.errors import DagsterInvariantViolationError
from dagster.core.storage.io_manager import IOManager


def n_asset_keys(path, n):
    return AssetPartitions(AssetKey(path), [str(i) for i in range(n)])


def test_output_definition_transitive_lineage():

    entry1 = EventMetadataEntry.int(123, "nrows")
    entry2 = EventMetadataEntry.float(3.21, "some value")

    @solid(output_defs=[OutputDefinition(name="output1", asset_key=lambda _: AssetKey("table1"))])
    def solid1(_):
        return Output(None, "output1", metadata_entries=[entry1])

    @solid
    def solidX(_, _input):
        return 1

    @solid(output_defs=[OutputDefinition(name="output3", asset_key=AssetKey("table3"))])
    def solid3(_, _input):
        yield Output(
            7,
            "output3",
            metadata_entries=[entry2],
        )

    @pipeline
    def my_pipeline():
        # attach an asset to an output
        out1 = solid1()
        outX = out1
        # 10 solids later,
        for i in range(10):
            outX = solidX.alias(f"solidX_{i}")(outX)
        solid3(outX)

    result = execute_pipeline(my_pipeline)
    events = result.step_event_list
    materializations = [
        event for event in events if event.event_type_value == "STEP_MATERIALIZATION"
    ]
    assert len(materializations) == 2

    event_data1 = materializations[0].event_specific_data
    assert event_data1.materialization.asset_key == AssetKey(["table1"])
    assert event_data1.materialization.metadata_entries == [entry1]
    assert event_data1.parent_assets == []

    event_data2 = materializations[1].event_specific_data
    assert event_data2.materialization.asset_key == AssetKey(["table3"])
    assert event_data2.materialization.metadata_entries == [entry2]
    assert event_data2.parent_assets == [AssetPartitions(AssetKey(["table1"]))]


def test_io_manager_diamond_lineage():
    class MyIOManager(IOManager):
        def handle_output(self, context, obj):
            # store asset
            return

        def load_input(self, context):
            return None

        def get_output_asset_key(self, context):
            return AssetKey([context.step_key, context.name])

    @io_manager
    def my_io_manager(_):
        return MyIOManager()

    @solid(
        output_defs=[
            OutputDefinition(name="outputA", io_manager_key="asset_io_manager"),
            OutputDefinition(name="outputB", io_manager_key="asset_io_manager"),
        ]
    )
    def solid_produce(_):
        yield Output(None, "outputA")
        yield Output(None, "outputB")

    @solid
    def solid_transform(_, _input):
        return None

    @solid(output_defs=[OutputDefinition(name="outputC", io_manager_key="asset_io_manager")])
    def solid_combine(_, _inputA, _inputB):
        return Output(None, "outputC")

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"asset_io_manager": my_io_manager})])
    def my_pipeline():
        a, b = solid_produce()
        at = solid_transform.alias("a_transform")(a)
        bt = solid_transform.alias("b_transform")(b)
        solid_combine(at, bt)

    result = execute_pipeline(my_pipeline)
    events = result.step_event_list
    materializations = [
        event for event in events if event.event_type_value == "STEP_MATERIALIZATION"
    ]
    assert len(materializations) == 3

    event_data1 = materializations[0].event_specific_data
    assert event_data1.materialization.asset_key == AssetKey(["solid_produce", "outputA"])
    assert event_data1.parent_assets == []

    event_data2 = materializations[1].event_specific_data
    assert event_data2.materialization.asset_key == AssetKey(["solid_produce", "outputB"])
    assert event_data2.parent_assets == []

    event_data3 = materializations[2].event_specific_data
    assert event_data3.materialization.asset_key == AssetKey(["solid_combine", "outputC"])
    assert event_data3.parent_assets == [
        AssetPartitions(AssetKey(["solid_produce", "outputA"])),
        AssetPartitions(AssetKey(["solid_produce", "outputB"])),
    ]


def test_multiple_definition_fails():
    class MyIOManager(IOManager):
        def handle_output(self, context, obj):
            # store asset
            return

        def load_input(self, context):
            return None

        def get_output_asset_key(self, context):
            return AssetKey([context.step_key, context.name])

    @io_manager
    def my_io_manager(_):
        return MyIOManager()

    @solid(
        output_defs=[
            OutputDefinition(asset_key=AssetKey("x"), io_manager_key="asset_io_manager"),
        ]
    )
    def fail_solid(_):
        return 1

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"asset_io_manager": my_io_manager})])
    def my_pipeline():
        fail_solid()

    with pytest.raises(DagsterInvariantViolationError):
        execute_pipeline(my_pipeline)


def test_input_definition_multiple_partition_lineage():

    entry1 = EventMetadataEntry.int(123, "nrows")
    entry2 = EventMetadataEntry.float(3.21, "some value")

    partition_entries = [EventMetadataEntry.int(123 * i * i, "partition count") for i in range(3)]

    @solid(
        output_defs=[
            OutputDefinition(
                name="output1",
                asset_key=AssetKey("table1"),
                asset_partitions=[str(i) for i in range(3)],
            )
        ],
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
        input_defs=[
            # here, only take 1 of the asset keys specified by the output
            InputDefinition(name="_input1", asset_fn=lambda _: n_asset_keys("table1", 1))
        ],
        output_defs=[OutputDefinition(name="output2", asset_key=lambda _: AssetKey("table2"))],
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
        assert event_data.materialization.asset_key == AssetKey(["table1"])
        assert event_data.materialization.metadata_entries == [entry1, partition_entries[i]]
        assert event_data.parent_assets == []

    event_data2 = materializations[-1].event_specific_data
    assert event_data2.materialization.asset_key == AssetKey(["table2"])
    assert event_data2.materialization.metadata_entries == [entry2]
    assert event_data2.parent_assets == [n_asset_keys("table1", 1)]
