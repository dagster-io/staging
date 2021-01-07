import tempfile
from typing import NamedTuple

import pytest
from dagster import (
    DagsterInstance,
    EventMetadataEntry,
    InputDefinition,
    InputManagerDefinition,
    ModeDefinition,
    ObjectManager,
    OutputDefinition,
    PythonObjectDagsterType,
    dagster_type_loader,
    execute_pipeline,
    fs_object_manager,
    input_manager,
    pipeline,
    resource,
    solid,
    usable_as_dagster_type,
)
from dagster.core.definitions.events import Failure, RetryRequested
from dagster.core.errors import DagsterInvalidSubsetError
from dagster.core.instance import InstanceRef
from dagster.core.storage.input_manager import type_based_root_input_manager


def test_validate_inputs():
    @input_manager
    def my_loader(_):
        return 5

    @solid(
        input_defs=[
            InputDefinition(
                "input1", dagster_type=PythonObjectDagsterType(int), root_manager_key="my_loader"
            )
        ]
    )
    def my_solid(_, input1):
        return input1

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"my_loader": my_loader})])
    def my_pipeline():
        my_solid()

    execute_pipeline(my_pipeline)


def test_root_input_manager():
    @input_manager
    def my_hardcoded_csv_loader(_context):
        return 5

    @solid(input_defs=[InputDefinition("input1", root_manager_key="my_loader")])
    def solid1(_, input1):
        assert input1 == 5

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"my_loader": my_hardcoded_csv_loader})])
    def my_pipeline():
        solid1()

    execute_pipeline(my_pipeline)


def test_configurable_root_input_manager():
    @input_manager(config_schema={"base_dir": str}, input_config_schema={"value": int})
    def my_configurable_csv_loader(context):
        assert context.resource_config["base_dir"] == "abc"
        return context.config["value"]

    @solid(input_defs=[InputDefinition("input1", root_manager_key="my_loader")])
    def solid1(_, input1):
        assert input1 == 5

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"my_loader": my_configurable_csv_loader})])
    def my_configurable_pipeline():
        solid1()

    execute_pipeline(
        my_configurable_pipeline,
        run_config={
            "solids": {"solid1": {"inputs": {"input1": {"value": 5}}}},
            "resources": {"my_loader": {"config": {"base_dir": "abc"}}},
        },
    )


def test_only_used_for_root():
    metadata = {"name": 5}

    class MyObjectManager(ObjectManager):
        def handle_output(self, context, obj):
            pass

        def load_input(self, context):
            output = context.upstream_output
            assert output.metadata == metadata
            assert output.name == "my_output"
            assert output.step_key == "solid1"
            assert context.pipeline_name == "my_pipeline"
            assert context.solid_def.name == solid2.name
            return 5

    @resource
    def my_object_manager(_):
        return MyObjectManager()

    @solid(
        output_defs=[
            OutputDefinition(name="my_output", manager_key="my_object_manager", metadata=metadata)
        ]
    )
    def solid1(_):
        return 1

    @solid(input_defs=[InputDefinition("input1", root_manager_key="my_root_manager")])
    def solid2(_, input1):
        assert input1 == 5

    @input_manager
    def root_manager(_):
        assert False, "should not be called"

    @pipeline(
        mode_defs=[
            ModeDefinition(
                resource_defs={
                    "my_object_manager": my_object_manager,
                    "my_root_manager": root_manager,
                }
            )
        ]
    )
    def my_pipeline():
        solid2(solid1())

    execute_pipeline(my_pipeline)


def test_configured():
    @input_manager(
        config_schema={"base_dir": str},
        description="abc",
        input_config_schema={"format": str},
        required_resource_keys={"r1", "r2"},
        version="123",
    )
    def my_input_manager(_):
        pass

    configured_input_manager = my_input_manager.configured({"base_dir": "/a/b/c"})

    assert isinstance(configured_input_manager, InputManagerDefinition)
    assert configured_input_manager.description == my_input_manager.description
    assert (
        configured_input_manager.required_resource_keys == my_input_manager.required_resource_keys
    )
    assert configured_input_manager.version is None


def test_input_manager_with_failure():
    @input_manager
    def should_fail(_):
        raise Failure(
            description="Foolure",
            metadata_entries=[
                EventMetadataEntry.text(label="label", text="text", description="description")
            ],
        )

    @solid(input_defs=[InputDefinition("_fail_input", root_manager_key="should_fail")])
    def fail_on_input(_, _fail_input):
        assert False, "should not be called"

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"should_fail": should_fail})])
    def simple():
        fail_on_input()

    with tempfile.TemporaryDirectory() as tmpdir_path:

        instance = DagsterInstance.from_ref(InstanceRef.from_dir(tmpdir_path))

        result = execute_pipeline(simple, instance=instance, raise_on_error=False)

        assert not result.success

        failure_data = result.result_for_solid("fail_on_input").failure_data

        assert failure_data.error.cls_name == "Failure"

        assert failure_data.user_failure_data.description == "Foolure"
        assert failure_data.user_failure_data.metadata_entries[0].label == "label"
        assert failure_data.user_failure_data.metadata_entries[0].entry_data.text == "text"
        assert failure_data.user_failure_data.metadata_entries[0].description == "description"


def test_input_manager_with_retries():
    _count = {"total": 0}

    @input_manager
    def should_succeed_after_retries(_):
        if _count["total"] < 2:
            _count["total"] += 1
            raise RetryRequested(max_retries=3)
        return "foo"

    @input_manager
    def should_retry(_):
        raise RetryRequested(max_retries=3)

    @solid(
        input_defs=[InputDefinition("solid_input", root_manager_key="should_succeed_after_retries")]
    )
    def take_input_1(_, solid_input):
        return solid_input

    @solid(input_defs=[InputDefinition("solid_input", root_manager_key="should_retry")])
    def take_input_2(_, solid_input):
        return solid_input

    @solid
    def take_input_3(_, _input1, _input2):
        assert False, "should not be called"

    @pipeline(
        mode_defs=[
            ModeDefinition(
                resource_defs={
                    "should_succeed_after_retries": should_succeed_after_retries,
                    "should_retry": should_retry,
                }
            )
        ]
    )
    def simple():
        take_input_3(take_input_2(), take_input_1())

    with tempfile.TemporaryDirectory() as tmpdir_path:

        instance = DagsterInstance.from_ref(InstanceRef.from_dir(tmpdir_path))

        result = execute_pipeline(simple, instance=instance, raise_on_error=False)

        step_stats = instance.get_run_step_stats(result.run_id)
        assert len(step_stats) == 2

        step_stats_1 = instance.get_run_step_stats(result.run_id, step_keys=["take_input_1"])
        assert len(step_stats_1) == 1
        step_stat_1 = step_stats_1[0]
        assert step_stat_1.status.value == "SUCCESS"
        assert step_stat_1.attempts == 3

        step_stats_2 = instance.get_run_step_stats(result.run_id, step_keys=["take_input_2"])
        assert len(step_stats_2) == 1
        step_stat_2 = step_stats_2[0]
        assert step_stat_2.status.value == "FAILURE"
        assert step_stat_2.attempts == 4

        step_stats_3 = instance.get_run_step_stats(result.run_id, step_keys=["take_input_3"])
        assert len(step_stats_3) == 0


def test_fan_in():
    with tempfile.TemporaryDirectory() as tmpdir_path:
        object_manager = fs_object_manager.configured({"base_dir": tmpdir_path})

        @solid
        def input_solid1(_):
            return 1

        @solid
        def input_solid2(_):
            return 2

        @solid(input_defs=[InputDefinition("input1", root_manager_key="input_manager")])
        def solid1(_, input1):
            assert input1 == [1, 2]

        @pipeline(
            mode_defs=[
                ModeDefinition(
                    resource_defs={
                        "object_manager": object_manager,
                        "input_manager": object_manager,
                    }
                )
            ]
        )
        def my_pipeline():
            solid1(input1=[input_solid1(), input_solid2()])

        execute_pipeline(my_pipeline)


def test_input_manager_resource_config():
    @input_manager(config_schema={"dog": str})
    def emit_dog(context):
        assert context.resource_config["dog"] == "poodle"

    @solid(input_defs=[InputDefinition("solid_input", root_manager_key="emit_dog")])
    def source_solid(_, solid_input):
        return solid_input

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"emit_dog": emit_dog})])
    def basic_pipeline():
        return source_solid(source_solid())

    result = execute_pipeline(
        basic_pipeline, run_config={"resources": {"emit_dog": {"config": {"dog": "poodle"}}}}
    )

    assert result.success


def test_custom_configurable_input_type_old():
    @dagster_type_loader(config_schema={"num_rows": int, "best_row": str})
    def file_summary_loader(_, config):
        return FileSummary(config["num_rows"], config["best_row"])

    @usable_as_dagster_type(loader=file_summary_loader)
    class FileSummary(NamedTuple):
        num_rows: int
        best_row: str

    @solid
    def summarize_file(_) -> FileSummary:
        """Summarize a file"""

    @solid(input_defs=[InputDefinition("file_summary", dagster_type=FileSummary)])
    def email_file_summary(context, file_summary):
        """Email a number to someone"""
        context.log.info(str(file_summary))

    @pipeline
    def my_pipeline():
        email_file_summary(summarize_file())

    execute_pipeline(
        my_pipeline,
        solid_selection=["email_file_summary"],
        run_config={
            "solids": {
                "email_file_summary": {
                    "inputs": {"file_summary": {"num_rows": 20, "best_row": "row5"}}
                }
            }
        },
    )


def test_custom_configurable_input_type():
    @dagster_type_loader(config_schema={"num_rows": int, "best_row": str})
    def file_summary_loader(_, config):
        return FileSummary(config["num_rows"], config["best_row"])

    @usable_as_dagster_type
    class FileSummary(NamedTuple):
        num_rows: int
        best_row: str

    @solid
    def summarize_file(_) -> FileSummary:
        return FileSummary(1, "fds")

    @solid(input_defs=[InputDefinition("file_summary", dagster_type=FileSummary)])
    def email_file_summary(context, file_summary):
        context.log.info(str(file_summary))

    input_manager_with_custom_types = type_based_root_input_manager(
        [(FileSummary, file_summary_loader)]
    )

    @pipeline(
        mode_defs=[ModeDefinition(resource_defs={"input_manager": input_manager_with_custom_types})]
    )
    def my_pipeline():
        email_file_summary(summarize_file())

    execute_pipeline(
        my_pipeline,
        solid_selection=["email_file_summary"],
        run_config={
            "solids": {
                "email_file_summary": {
                    "inputs": {"file_summary": {"num_rows": 20, "best_row": "row5"}}
                }
            }
        },
    )

    execute_pipeline(my_pipeline)


def test_unhandled_type():
    @usable_as_dagster_type(name="InputTypeWithoutHydration")
    class InputTypeWithoutHydration(int):
        pass

    @solid(output_defs=[OutputDefinition(InputTypeWithoutHydration)])
    def one(_):
        return 1

    @solid(input_defs=[InputDefinition("some_input", InputTypeWithoutHydration)])
    def fail_subset(_, some_input):
        return some_input

    @pipeline
    def my_pipeline():
        fail_subset(one())

    with pytest.raises(DagsterInvalidSubsetError):
        my_pipeline.get_pipeline_subset_def({"fail_subset"})
