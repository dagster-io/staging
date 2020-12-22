from dagster import (
    InputDefinition,
    InputManagerDefinition,
    ModeDefinition,
    ObjectManager,
    OutputDefinition,
    PythonObjectDagsterType,
    execute_pipeline,
    pipeline,
    resource,
    solid,
)
from dagster.core.storage.input_manager import input_manager


def test_validate_inputs():
    @input_manager
    def my_loader(_, _resource_config):
        return 5

    @solid(
        input_defs=[
            InputDefinition(
                "input1", dagster_type=PythonObjectDagsterType(int), manager_key="my_loader"
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
    def my_hardcoded_csv_loader(_context, _resource_config):
        return 5

    @solid(input_defs=[InputDefinition("input1", manager_key="my_loader")])
    def solid1(_, input1):
        assert input1 == 5

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"my_loader": my_hardcoded_csv_loader})])
    def my_pipeline():
        solid1()

    execute_pipeline(my_pipeline)


def test_configurable_root_input_manager():
    @input_manager(config_schema={"base_dir": str}, input_config_schema={"value": int})
    def my_configurable_csv_loader(context, resource_config):
        assert resource_config["base_dir"] == "abc"
        return context.input_config["value"]

    @solid(input_defs=[InputDefinition("input1", manager_key="my_loader")])
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


def test_override_object_manager():
    metadata = {"name": 5}

    class MyObjectManager(ObjectManager):
        def handle_output(self, context, obj):
            pass

        def load_input(self, context):
            assert False, "should not be called"

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

    @solid(input_defs=[InputDefinition("input1", manager_key="spark_loader")])
    def solid2(_, input1):
        assert input1 == 5

    @input_manager
    def spark_table_loader(context, _resource_config):
        output = context.upstream_output
        assert output.metadata == metadata
        assert output.name == "my_output"
        assert output.step_key == "solid1"
        assert context.pipeline_name == "my_pipeline"
        assert context.solid_def.name == solid2.name
        return 5

    @pipeline(
        mode_defs=[
            ModeDefinition(
                resource_defs={
                    "my_object_manager": my_object_manager,
                    "spark_loader": spark_table_loader,
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
    assert configured_input_manager.input_config_schema == my_input_manager.input_config_schema
    assert (
        configured_input_manager.required_resource_keys == my_input_manager.required_resource_keys
    )
    assert configured_input_manager.version is None


def test_type_based_config_schema():
    from dagster import String, Int

    def get_config_schema(input_def):
        if input_def.dagster_type == String:
            return str
        elif input_def.dagster_type == Int:
            return int

    @input_manager(input_config_schema=get_config_schema)
    def my_input_manager(context, _resource_config):
        return context.input_config + 1

    @solid(input_defs=[InputDefinition("my_input", manager_key="my_manager", dagster_type=Int)])
    def my_solid(_, my_input):
        assert my_input == 6

    @pipeline
    def my_pipeline():
        my_solid()

    execute_pipeline(my_pipeline, run_config={"solids": {"my_solid": {"inputs": {"my_input": 5}}}})


def test_configurable_inputs_pipeline():
    @solid
    def download_file(_) -> str:
        """Download a file and return its path"""

    @solid
    def summarize_file(_, file_path) -> float:
        """Summarize a file with a magical number"""

    @solid
    def email_file_summary(_, file_summary):
        """Email a number to someone"""

    @pipeline
    def my_pipeline():
        email_file_summary(summarize_file(download_file))

    execute_pipeline(
        my_pipeline,
        solid_selection=["summarize_file", "email_file_summary"],
        run_config={"solids": {"summarize_file": {"inputs": {"file_path": "a/b/c"}}}},
    )


def test_custom_configurable_input_type_old():
    from dagster import dagster_type_loader, usable_as_dagster_type
    from dataclasses import dataclass

    @dagster_type_loader(config_schema={"num_rows": int, "best_row": str})
    def file_summary_loader(context, config):
        return FileSummary(config["num_rows"], config["best_row"])

    @usable_as_dagster_type(loader=file_summary_loader)
    @dataclass
    class FileSummary:
        num_rows: int
        best_row: str

    @solid
    def summarize_file(_) -> FileSummary:
        """Summarize a file"""

    @solid
    def email_file_summary(_, file_summary):
        """Email a number to someone"""

    input_manager_with_custom_types = make_upstream_input_manager(
        type_input_manager_defs={FileSummary: file_summary_loader}
    )

    @pipeline(
        mode_defs=[ModeDefinition(resource_defs={"input_manager": input_manager_with_custom_types})]
    )
    def my_pipeline():
        email_file_summary(summarize_file())

    execute_pipeline(
        my_pipeline,
        solid_selection=["email_file_summary"],
        run_config={"solids": {"email_file_summary": {"inputs": {"file_summary": "a/b/c"}}}},
    )


# This is an API that Dagster would provide.  Here it's defined in user space
# for convenience.
def make_upstream_input_manager(type_input_manager_defs):
    """
    It likely makes sense to introduce some kind of @loader definition that's not a resource.

    For now, assume that none of given loader definitions have required resources keys or resource
    config.
    """
    from dagster.core.types.dagster_type import resolve_dagster_type
    from dagster import InputManager

    input_manager_defs_by_type_name = {
        resolve_dagster_type(dagster_type).unique_name: input_manager_def
        for dagster_type, input_manager_def in type_input_manager_defs
    }

    def config_schema_fn(input_def, has_upstream):
        if has_upstream:
            return None

        type_input_manager_def = input_manager_defs_by_type_name.get(
            input_def.dagster_type.unique_name
        )
        return type_input_manager_def.get_input_config_schema(input_def, has_upstream)

    class UpstreamInputManagerWithTypes(InputManager):
        def __init__(self, input_managers_by_type_name):
            self.input_managers_by_type_name = input_managers_by_type_name

        def load_input(self, context):
            if context.upstream_output:
                return context.upstream_output_manager.load_input(context)
            else:
                return self.input_managers_by_type_name[
                    context.dagster_type.unique_name
                ].load_input(context)

    def init_input_manager_fn(init_context):
        input_managers_by_type_name = {
            type_name: input_manager_def.resource_fn(init_context)
            for type_name, input_manager_def in input_manager_defs_by_type_name.items()
        }
        return UpstreamInputManagerWithTypes(input_managers_by_type_name)

    return InputManagerDefinition(
        resource_fn=init_input_manager_fn, input_config_schema_fn=config_schema_fn
    )


def test_custom_configurable_input_type():
    from dagster import usable_as_dagster_type
    from dataclasses import dataclass

    # It likely makes sense to introduce some kind of @loader definition that's not a resource.
    @input_manager(input_config_schema={"num_rows": int, "best_row": str})
    def file_summary_loader(context, _resource_config):
        return FileSummary(context.input_config["num_rows"], context.input_config["best_row"])

    @usable_as_dagster_type
    @dataclass
    class FileSummary:
        num_rows: int
        best_row: str

    @solid
    def summarize_file(_) -> FileSummary:
        return FileSummary(1, "fds")

    @solid(input_defs=[InputDefinition("file_summary", dagster_type=FileSummary)])
    def email_file_summary(_, file_summary):
        """Email a number to someone"""

    input_manager_with_custom_types = make_upstream_input_manager(
        type_input_manager_defs=[(FileSummary, file_summary_loader)]
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
