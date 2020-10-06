import os
import pickle

import pytest

from dagster import (
    AssetMaterialization,
    InputDefinition,
    Int,
    Output,
    OutputDefinition,
    String,
    dagster_type_loader,
    dagster_type_materializer,
    execute_pipeline,
    reexecute_pipeline,
    seven,
    solid,
)
from dagster.core.definitions import pipeline
from dagster.core.definitions.address import Address
from dagster.core.definitions.events import AddressMetadataEntryData
from dagster.core.errors import DagsterAddressIOError
from dagster.core.execution.plan.objects import StepOutputHandle
from dagster.core.instance import DagsterInstance
from dagster.core.storage.intermediate_storage import build_fs_intermediate_storage
from dagster.core.storage.object_store import ObjectStoreOperationType
from dagster.core.types.dagster_type import create_any_type


def test_address_path_operation_using_intermediates_file_system():
    with seven.TemporaryDirectory() as tmpdir_path:
        output_address = os.path.join(tmpdir_path, "solid1.output")
        output_value = 5

        instance = DagsterInstance.ephemeral()
        intermediate_storage = build_fs_intermediate_storage(
            instance.intermediates_directory, run_id="some_run_id"
        )

        object_operation_result = intermediate_storage.set_intermediate(
            context=None,
            dagster_type=Int,
            step_output_handle=StepOutputHandle("solid1.compute"),
            value=output_value,
            address=Address(path=output_address),
        )

        assert object_operation_result.key == output_address
        assert object_operation_result.obj == output_value

        assert (
            intermediate_storage.get_intermediate(
                context=None,
                dagster_type=Int,
                step_output_handle=StepOutputHandle("solid1.compute"),
                address=Address(path=output_address),
            ).obj
            == output_value
        )

        with pytest.raises(
            DagsterAddressIOError, match="No such file or directory",
        ):
            intermediate_storage.set_intermediate(
                context=None,
                dagster_type=Int,
                step_output_handle=StepOutputHandle("solid1.compute"),
                value=1,
                address=Address(path="invalid_address"),
            )

        with pytest.raises(
            DagsterAddressIOError, match="No such file or directory",
        ):
            intermediate_storage.get_intermediate(
                context=None,
                dagster_type=Int,
                step_output_handle=StepOutputHandle("solid1.compute"),
                address=Address(path=os.path.join(tmpdir_path, "invalid.output")),
            )


@dagster_type_loader(String)
def my_loader(_, path):
    with open(path, "rb") as fd:
        return pickle.load(fd)


@dagster_type_materializer(String)
def my_materializer(_, path, value):
    with open(path, "wb") as f:
        pickle.dump(value, f)
    return AssetMaterialization.file(path)


CustomType = create_any_type(name="MyType", materializer=my_materializer, loader=my_loader)


def test_address_config_value_operation_run_config():
    @solid(
        input_defs=[InputDefinition("val", CustomType)], output_defs=[OutputDefinition(CustomType)]
    )
    def solid_a(_, val):
        return val

    @solid(input_defs=[InputDefinition("val", CustomType)])
    def solid_b(_, val):
        assert val == [1, 2, 3]

    @pipeline
    def foo():
        solid_b(solid_a())

    instance = DagsterInstance.ephemeral()
    with seven.TemporaryDirectory() as tempdir:
        data = [1, 2, 3]
        file_path = os.path.join(tempdir, "foo")

        with open(file_path, "wb") as f:
            pickle.dump(data, f)
        intermediate_output_path = os.path.join(tempdir, "intermediate")
        run_config = {
            "solids": {
                "solid_a": {
                    "inputs": {"val": file_path},
                    "outputs": [{"result": intermediate_output_path}],
                }
            },
            "storage": {"filesystem": None},
        }
        result = execute_pipeline(foo, run_config=run_config, instance=instance,)

        assert result.success
        intermediate_val = result.result_for_solid("solid_a").output_value()
        assert intermediate_val == [1, 2, 3]

        # make sure data is passed by reference between solids
        external_intermediate_events = list(
            filter(lambda evt: evt.is_external_operation_event, result.event_list)
        )
        assert len(external_intermediate_events) == 2
        # SET_EXTERNAL_OBJECT
        assert (
            external_intermediate_events[0].event_specific_data.op
            == ObjectStoreOperationType.SET_EXTERNAL_OBJECT.value
        )
        for entry_data in external_intermediate_events[1].event_specific_data.metadata_entries:
            if isinstance(entry_data, AddressMetadataEntryData):
                assert entry_data.address.config_value == intermediate_output_path
        # GET_EXTERNAL_OBJECT
        assert (
            external_intermediate_events[1].event_specific_data.op
            == ObjectStoreOperationType.GET_EXTERNAL_OBJECT.value
        )
        for entry_data in external_intermediate_events[1].event_specific_data.metadata_entries:
            if isinstance(entry_data, AddressMetadataEntryData):
                assert entry_data.address.config_value == intermediate_output_path

        # test cross-run external_intermediates
        reexecution_result = reexecute_pipeline(
            foo,
            result.run_id,
            instance=instance,
            run_config=run_config,
            step_selection=["solid_b.compute"],
        )

        assert reexecution_result.success

        get_external_object_events = list(
            filter(lambda evt: evt.is_external_operation_event, reexecution_result.event_list)
        )
        assert len(get_external_object_events) == 1
        assert (
            get_external_object_events[0].event_specific_data.op
            == ObjectStoreOperationType.GET_EXTERNAL_OBJECT.value
        )


def test_address_config_value_operation_in_solid():
    with seven.TemporaryDirectory() as tempdir:
        intermediate_output_path = os.path.join(tempdir, "intermediate")

        @solid(output_defs=[OutputDefinition(CustomType)])
        def solid_a(_):
            return Output([1, 2, 3], address=Address(config_value=intermediate_output_path))

        @solid(input_defs=[InputDefinition("val", CustomType)])
        def solid_b(_, val):
            assert val == [1, 2, 3]

        @pipeline
        def foo():
            solid_b(solid_a())

        result = execute_pipeline(foo)
        assert result.success

        # verify output is also the intermediate
        with open(os.path.join(tempdir, "intermediate"), "rb") as fd:
            output_val = pickle.load(fd)

        intermediate_val = result.result_for_solid("solid_a").output_value()
        assert intermediate_val == [1, 2, 3]
        assert intermediate_val == output_val

        # make sure data is passed by reference between solids
        external_intermediate_events = list(
            filter(lambda evt: evt.is_external_operation_event, result.event_list)
        )
        assert len(external_intermediate_events) == 2
        # SET_EXTERNAL_OBJECT
        assert (
            external_intermediate_events[0].event_specific_data.op
            == ObjectStoreOperationType.SET_EXTERNAL_OBJECT.value
        )
        for entry_data in external_intermediate_events[1].event_specific_data.metadata_entries:
            if isinstance(entry_data, AddressMetadataEntryData):
                assert entry_data.address.config_value == intermediate_output_path
        # GET_EXTERNAL_OBJECT
        assert (
            external_intermediate_events[1].event_specific_data.op
            == ObjectStoreOperationType.GET_EXTERNAL_OBJECT.value
        )
        for entry_data in external_intermediate_events[1].event_specific_data.metadata_entries:
            if isinstance(entry_data, AddressMetadataEntryData):
                assert entry_data.address.config_value == intermediate_output_path
