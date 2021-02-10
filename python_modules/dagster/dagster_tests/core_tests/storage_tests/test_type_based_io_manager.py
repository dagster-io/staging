import tempfile

import pytest
from dagster import (
    DagsterInvariantViolationError,
    IOManager,
    InputDefinition,
    ModeDefinition,
    Output,
    OutputDefinition,
    execute_pipeline,
    fs_io_manager,
    io_manager,
    mem_io_manager,
    pipeline,
    solid,
    usable_as_dagster_type,
)
from dagster.check import CheckError
from dagster.core.storage.type_based_io_manager import type_based_io_manager


@usable_as_dagster_type
class Type1:
    pass


@usable_as_dagster_type
class Type2:
    pass


def test_handle_output():
    calls = {}

    class ValueIOManager(IOManager):
        def __init__(self, value):
            self.value = value

        def handle_output(self, context, _obj):
            calls[context.name] = self.value

        def load_input(self, _):
            raise NotImplementedError()

    @io_manager
    def io_manager1(_):
        return ValueIOManager(1)

    @io_manager
    def io_manager2(_):
        return ValueIOManager(2)

    my_io_manager = type_based_io_manager([(Type1, io_manager1), (Type2, io_manager2)])

    @solid(output_defs=[OutputDefinition(Type1, "output1"), OutputDefinition(Type2, "output2")])
    def my_solid(_):
        yield Output(Type1(), "output1")
        yield Output(Type2(), "output2")

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"io_manager": my_io_manager})])
    def my_pipeline():
        my_solid()

    execute_pipeline(my_pipeline)

    assert calls["output1"] == 1
    assert calls["output2"] == 2


def test_load_input():
    class ValueIOManager(IOManager):
        def __init__(self, value):
            self.value = value

        def handle_output(self, _context, _obj):
            pass

        def load_input(self, context):
            if self.value == 1:
                return Type1()
            elif self.value == 2:
                return Type2()
            else:
                assert False

    @io_manager
    def io_manager1(_):
        return ValueIOManager(1)

    @io_manager
    def io_manager2(_):
        return ValueIOManager(2)

    my_io_manager = type_based_io_manager(
        [(Type1, io_manager1), (Type2, io_manager2)], default_manager_def=mem_io_manager
    )

    @solid(output_defs=[OutputDefinition(name="output1"), OutputDefinition(name="output2")])
    def my_solid(_):
        yield Output(None, "output1")
        yield Output(None, "output2")

    @solid(input_defs=[InputDefinition("input1", Type1), InputDefinition("input2", Type2)])
    def downstream_solid(_, input1, input2):
        assert isinstance(input1, Type1)
        assert isinstance(input2, Type2)

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"io_manager": my_io_manager})])
    def my_pipeline():
        output1, output2 = my_solid()
        downstream_solid(output1, output2)

    execute_pipeline(my_pipeline)


def test_handle_output_with_resource_config():
    calls = {}

    class ValueIOManager(IOManager):
        def __init__(self, value):
            self.value = value

        def handle_output(self, context, _obj):
            assert context.resource_config["value"] == self.value
            calls[context.name] = self.value

        def load_input(self, _):
            raise NotImplementedError()

    @io_manager(config_schema={"value": int})
    def io_manager1(_):
        return ValueIOManager(1)

    @io_manager(config_schema={"value": int})
    def io_manager2(context):
        assert context.resource_config["value"] == 2
        return ValueIOManager(2)

    my_io_manager = type_based_io_manager([(Type1, io_manager1), (Type2, io_manager2)])

    @solid(output_defs=[OutputDefinition(Type1, "output1"), OutputDefinition(Type2, "output2")])
    def my_solid(_):
        yield Output(Type1(), "output1")
        yield Output(Type2(), "output2")

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"io_manager": my_io_manager})])
    def my_pipeline():
        my_solid()

    execute_pipeline(
        my_pipeline,
        run_config={
            "resources": {"io_manager": {"config": {"Type1": {"value": 1}, "Type2": {"value": 2}}}}
        },
    )

    assert calls["output1"] == 1
    assert calls["output2"] == 2


def test_default_manager_def():
    with tempfile.TemporaryDirectory() as tmpdir_path:
        my_io_manager = type_based_io_manager([], default_manager_def=fs_io_manager).configured(
            {"default": {"base_dir": tmpdir_path}}
        )

        @solid
        def my_solid(_):
            return 1

        @solid
        def my_solid_2(_, input_val):
            assert input_val == 1

        @pipeline(mode_defs=[ModeDefinition(resource_defs={"io_manager": my_io_manager})])
        def my_pipeline():
            my_solid_2(my_solid())

        execute_pipeline(my_pipeline)


def test_configured_type_manager():
    with pytest.raises(CheckError):
        type_based_io_manager([(str, fs_io_manager.configured({"base_dir": "abc"}))])


def test_configured_default():
    with pytest.raises(CheckError):
        type_based_io_manager([], default_manager_def=fs_io_manager.configured({"base_dir": "abc"}))


def test_missing_type_no_default():
    with tempfile.TemporaryDirectory() as tmpdir_path:
        my_io_manager = type_based_io_manager([(str, fs_io_manager)]).configured(
            {"String": {"base_dir": tmpdir_path}}
        )

        @solid
        def my_solid(_):
            return 1

        @pipeline(mode_defs=[ModeDefinition(resource_defs={"io_manager": my_io_manager})])
        def my_pipeline():
            my_solid()

        with pytest.raises(DagsterInvariantViolationError):
            execute_pipeline(my_pipeline)
