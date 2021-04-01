import pytest
from dagster import (
    AssetKey,
    InputDefinition,
    ModeDefinition,
    Output,
    OutputDefinition,
    composite_solid,
    execute_pipeline,
    fs_io_manager,
    io_manager,
    pipeline,
    reconstructable,
    solid,
)
from dagster.core.definitions.events import (
    AssetLineageInfo,
    EventMetadataEntry,
    PartitionMetadataEntry,
)
from dagster.core.errors import DagsterInvariantViolationError
from dagster.core.execution.context.system import AssetInputHandle, AssetOutputHandle
from dagster.core.storage.fs_io_manager import PickledObjectFilesystemIOManager
from dagster.core.storage.io_manager import IOManager
from dagster.core.test_utils import instance_for_test
from dagster.experimental import DynamicOutput, DynamicOutputDefinition


def n_asset_keys(path, n):
    return AssetLineageInfo(AssetKey(path), set([str(i) for i in range(n)]))


class MyAssetIOManager(IOManager):
    def handle_output(self, context, obj):
        # store asset
        return

    def load_input(self, context):
        return None

    def get_output_asset_key(self, _context):
        # return AssetKey([context.step_key, context.name])
        return None


@io_manager(output_asset_key=lambda context: AssetKey([context.step_key, context.name]))
def simple_asset_io_manager(_):
    return MyAssetIOManager()


def check_materialization(
    materialization, asset_key, parent_assets=None, metadata_entries=None, ignore_asset_path=True
):
    event_data = materialization.event_specific_data
    assert event_data.materialization.asset_key == asset_key
    assert sorted(event_data.materialization.metadata_entries) == sorted(metadata_entries or [])
    if ignore_asset_path:
        assert len(event_data.asset_lineage) == len(parent_assets or [])
        for i in range(len(event_data.asset_lineage)):
            event_asset = event_data.asset_lineage[i]
            expected_asset = parent_assets[i]
            assert event_asset.asset_key == expected_asset.asset_key
            assert event_asset.partitions == expected_asset.partitions
    else:
        assert event_data.asset_lineage == (parent_assets or [])


def test_io_manager_diamond_lineage():
    @solid(
        output_defs=[
            OutputDefinition(name="outputA", io_manager_key="asset_io_manager"),
            OutputDefinition(name="outputB", io_manager_key="asset_io_manager"),
        ]
    )
    def solid_produce(_):
        yield Output(None, "outputA")
        yield Output(None, "outputB")

    @solid(output_defs=[OutputDefinition(name="outputT", io_manager_key="asset_io_manager")])
    def solid_transform(_, _input):
        return None

    @solid(output_defs=[OutputDefinition(name="outputC", io_manager_key="asset_io_manager")])
    def solid_combine(_, _inputA, _inputB):
        return Output(None, "outputC")

    @pipeline(
        mode_defs=[ModeDefinition(resource_defs={"asset_io_manager": simple_asset_io_manager})]
    )
    def my_pipeline():
        a, b = solid_produce()
        at = solid_transform.alias("a_transform")(a)
        bt = solid_transform.alias("b_transform")(b)
        solid_combine(at, bt)

    result = execute_pipeline(my_pipeline)
    events = result.step_event_list
    materializations = [
        event for event in events if event.event_type_value == "ASSET_MATERIALIZATION"
    ]
    assert len(materializations) == 5

    check_materialization(materializations[0], AssetKey(["solid_produce", "outputA"]))
    check_materialization(materializations[1], AssetKey(["solid_produce", "outputB"]))
    check_materialization(
        materializations[-1],
        AssetKey(
            ["solid_combine", "outputC"],
        ),
        parent_assets=[
            AssetLineageInfo(AssetKey(["a_transform", "outputT"])),
            AssetLineageInfo(AssetKey(["b_transform", "outputT"])),
        ],
    )


def test_io_manager_transitive_lineage():
    @solid(output_defs=[OutputDefinition(io_manager_key="asset_io_manager")])
    def solid_produce(_):
        return 1

    @solid
    def passthrough_solid(_, inp):
        return inp

    @solid(output_defs=[OutputDefinition(io_manager_key="asset_io_manager")])
    def solid_transform(_, inp):
        return inp

    @pipeline(
        mode_defs=[ModeDefinition(resource_defs={"asset_io_manager": simple_asset_io_manager})]
    )
    def my_pipeline():
        x = solid_produce()
        for i in range(10):
            x = passthrough_solid.alias(f"passthrough_{i}")(x)
        solid_transform(x)

    result = execute_pipeline(my_pipeline)
    events = result.step_event_list
    materializations = [
        event for event in events if event.event_type_value == "ASSET_MATERIALIZATION"
    ]

    expected_path = [AssetOutputHandle("solid_produce", "result", None)]
    for i in range(10):
        expected_path.extend(
            [
                AssetInputHandle(f"passthrough_{i}", "inp"),
                AssetOutputHandle(f"passthrough_{i}", "result", None),
            ]
        )
    expected_path.append(AssetInputHandle("solid_transform", "inp"))
    assert len(materializations) == 2

    check_materialization(materializations[0], AssetKey(["solid_produce", "result"]))
    check_materialization(
        materializations[1],
        AssetKey(["solid_transform", "result"]),
        parent_assets=[AssetLineageInfo(AssetKey(["solid_produce", "result"]), path=expected_path)],
        ignore_asset_path=False,
    )


def test_multiple_definition_fails():
    @solid(
        output_defs=[
            OutputDefinition(asset_key=AssetKey("x"), io_manager_key="asset_io_manager"),
        ]
    )
    def fail_solid(_):
        return 1

    @pipeline(
        mode_defs=[ModeDefinition(resource_defs={"asset_io_manager": simple_asset_io_manager})]
    )
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
                asset_partitions=set([str(i) for i in range(3)]),
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
                    PartitionMetadataEntry(str(i), entry)
                    for i, entry in enumerate(partition_entries)
                ],
            ],
        )

    @solid(
        input_defs=[
            # here, only take 1 of the asset keys specified by the output
            InputDefinition(
                name="_input1", asset_key=AssetKey("table1"), asset_partitions=set(["0"])
            )
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
        event for event in events if event.event_type_value == "ASSET_MATERIALIZATION"
    ]
    assert len(materializations) == 4

    seen_partitions = set()
    for i in range(3):
        partition = materializations[i].partition
        seen_partitions.add(partition)
        check_materialization(
            materializations[i],
            AssetKey(["table1"]),
            metadata_entries=[entry1, partition_entries[int(partition)]],
        )

    assert len(seen_partitions) == 3

    check_materialization(
        materializations[-1],
        AssetKey(["table2"]),
        parent_assets=[n_asset_keys("table1", 1)],
        metadata_entries=[entry2],
    )


def test_mixed_asset_definition_lineage():
    @solid(output_defs=[OutputDefinition(io_manager_key="asset_io_manager")])
    def io_manager_solid(_):
        return 1

    @solid(
        output_defs=[OutputDefinition(asset_key=AssetKey(["output_def_table", "output_def_solid"]))]
    )
    def output_def_solid(_):
        return 1

    @solid(
        output_defs=[
            OutputDefinition(name="a", asset_key=AssetKey(["output_def_table", "combine_solid"])),
            OutputDefinition(name="b", io_manager_key="asset_io_manager"),
        ]
    )
    def combine_solid(_, _a, _b):
        yield Output(None, "a")
        yield Output(None, "b")

    @pipeline(
        mode_defs=[ModeDefinition(resource_defs={"asset_io_manager": simple_asset_io_manager})]
    )
    def my_pipeline():
        a = io_manager_solid()
        b = output_def_solid()
        combine_solid(a, b)

    result = execute_pipeline(my_pipeline)
    events = result.step_event_list
    materializations = [
        event for event in events if event.event_type_value == "ASSET_MATERIALIZATION"
    ]
    assert len(materializations) == 4

    check_materialization(materializations[0], AssetKey(["io_manager_solid", "result"]))
    check_materialization(materializations[1], AssetKey(["output_def_table", "output_def_solid"]))
    check_materialization(
        materializations[2],
        AssetKey(["output_def_table", "combine_solid"]),
        parent_assets=[
            AssetLineageInfo(AssetKey(["io_manager_solid", "result"])),
            AssetLineageInfo(AssetKey(["output_def_table", "output_def_solid"])),
        ],
    )
    check_materialization(
        materializations[3],
        AssetKey(["combine_solid", "b"]),
        parent_assets=[
            AssetLineageInfo(AssetKey(["io_manager_solid", "result"])),
            AssetLineageInfo(AssetKey(["output_def_table", "output_def_solid"])),
        ],
    )


def test_dynamic_output_definition_single_partition_materialization():

    entry1 = EventMetadataEntry.int(123, "nrows")
    entry2 = EventMetadataEntry.float(3.21, "some value")

    @solid(output_defs=[OutputDefinition(name="output1", asset_key=AssetKey("table1"))])
    def solid1(_):
        return Output(None, "output1", metadata_entries=[entry1])

    @solid(
        output_defs=[
            DynamicOutputDefinition(
                name="output2", asset_key=lambda context: AssetKey(context.mapping_key)
            )
        ]
    )
    def solid2(_, _input1):
        for i in range(4):
            yield DynamicOutput(
                7,
                mapping_key=str(i),
                output_name="output2",
                metadata_entries=[entry2],
            )

    @solid
    def do_nothing(_, _input1):
        pass

    @pipeline
    def my_pipeline():
        solid2(solid1()).map(do_nothing)

    result = execute_pipeline(my_pipeline)
    events = result.step_event_list
    materializations = [
        event for event in events if event.event_type_value == "ASSET_MATERIALIZATION"
    ]
    assert len(materializations) == 5

    check_materialization(materializations[0], AssetKey(["table1"]), metadata_entries=[entry1])
    seen_paths = set()
    for i in range(1, 5):
        path = materializations[i].asset_key.path
        seen_paths.add(tuple(path))
        check_materialization(
            materializations[i],
            AssetKey(path),
            metadata_entries=[entry2],
            parent_assets=[AssetLineageInfo(AssetKey(["table1"]))],
        )
    assert len(seen_paths) == 4


"""
def test_subset_boundary_lineage():
    @solid(output_defs=[OutputDefinition(io_manager_key="asset_io_manager")])
    def solid1(_):
        return 1

    @solid(
        input_defs=[InputDefinition(name="_in", root_manager_key="asset_io_manager")],
        output_defs=[OutputDefinition(io_manager_key="asset_io_manager")],
    )
    def solid2(_, _in):
        return 1

    @pipeline(
        mode_defs=[ModeDefinition(resource_defs={"asset_io_manager": simple_asset_io_manager})]
    )
    def my_pipeline():
        solid2(solid1())

    result = execute_pipeline(my_pipeline, solid_selection=["solid2"])
    events = result.step_event_list
    materializations = [
        event for event in events if event.event_type_value == "ASSET_MATERIALIZATION"
    ]
    assert len(materializations) == 1

    check_materialization(
        materializations[0],
        AssetKey(["solid2", "result"]),
        parent_assets=[AssetLineageInfo(AssetKey(["solid1", "result"]))],
    )
"""


# ========================================

NSPLIT = 5


@io_manager(output_asset_key=lambda context: AssetKey(context.metadata["asset_key"]))
def my_fancy_io_manager_1(_):
    return PickledObjectFilesystemIOManager(base_dir="/tmp/")


@io_manager(output_asset_key=lambda context: AssetKey(context.metadata["asset_key"]))
def my_fancy_io_manager_2(_):
    return PickledObjectFilesystemIOManager(base_dir="/tmp/")


@solid(
    output_defs=[
        OutputDefinition(
            io_manager_key="fancy1",
            metadata={"asset_key": "load"},
        )
    ]
)
def my_loading_solid(_):
    return 1


@solid(output_defs=[OutputDefinition(name=f"out_{i}") for i in range(NSPLIT)])
def my_splitting_solid(_, inp):
    for i in range(NSPLIT):
        yield Output(inp, f"out_{i}")


@solid(output_defs=[DynamicOutputDefinition(name="out")])
def my_dynamic_splitting_solid(_, inp):
    for i in range(NSPLIT):
        yield DynamicOutput(inp, mapping_key=str(i), output_name="out")


@solid
def my_passthrough_solid(_, inp):
    return inp


@solid(
    output_defs=[
        OutputDefinition(
            io_manager_key="fancy2",
            metadata={"asset_key": "transform"},
        )
    ]
)
def my_transforming_solid(_, inp):
    return inp + 1


@composite_solid
def my_big_passthrough_solid(inp):
    return my_passthrough_solid(my_passthrough_solid(my_passthrough_solid(inp)))


@composite_solid
def my_composite_transforming_solid(inp):
    return my_transforming_solid(my_big_passthrough_solid(inp))


@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={
                "io_manager": fs_io_manager,
                "fancy1": my_fancy_io_manager_1,
                "fancy2": my_fancy_io_manager_2,
            }
        )
    ]
)
def my_complex_pipeline():
    split_vals = my_splitting_solid(my_loading_solid())
    for val in split_vals:
        my_transforming_solid(my_big_passthrough_solid(val))


@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={
                "io_manager": fs_io_manager,
                "fancy1": my_fancy_io_manager_1,
                "fancy2": my_fancy_io_manager_2,
            }
        )
    ]
)
def my_dynamic_complex_pipeline():
    split_vals = my_dynamic_splitting_solid(my_loading_solid())
    split_vals.map(my_composite_transforming_solid)


def test_multiproc_pipelines():

    for test_pipeline in [my_complex_pipeline]:  # , my_dynamic_complex_pipeline]:
        with instance_for_test() as instance:
            result = execute_pipeline(
                reconstructable(test_pipeline),
                run_config={"execution": {"multiprocess": {"config": {"max_concurrent": 4}}}},
                instance=instance,
            )
            events = result.step_event_list
            materializations = [
                event for event in events if event.event_type_value == "ASSET_MATERIALIZATION"
            ]
            assert len(materializations) == 1 + NSPLIT

            check_materialization(materializations[0], AssetKey(["load"]))
            for i in range(NSPLIT):
                check_materialization(
                    materializations[i + 1],
                    AssetKey(["transform"]),
                    parent_assets=[AssetLineageInfo(AssetKey("load"))],
                )
