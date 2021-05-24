from tempfile import TemporaryDirectory

from dagster import (
    Any,
    ModeDefinition,
    ResourceDefinition,
    execute_pipeline,
    io_manager,
    pipeline,
    solid,
)
from dagster.core.execution.build_resources import initialize_console_manager
from dagster.core.execution.context.input import InputContext
from dagster.core.execution.context.output import OutputContext
from dagster.core.storage.memoizable_io_manager import (
    MemoizableIOManager,
    VersionedPickledObjectFilesystemIOManager,
)
from dagster.core.storage.tags import MEMOIZED_RUN_TAG


def test_versioned_pickled_object_filesystem_io_manager():
    console_manager = initialize_console_manager(None)
    with TemporaryDirectory() as temp_dir:
        store = VersionedPickledObjectFilesystemIOManager(temp_dir)
        context = OutputContext(
            step_key="foo",
            name="bar",
            mapping_key=None,
            log_manager=console_manager,
            metadata={},
            pipeline_name="fake",
            solid_def=None,
            dagster_type=Any,
            run_id=None,
            version="version1",
        )
        store.handle_output(context, "cat")
        assert store.has_output(context)
        assert (
            store.load_input(
                InputContext(
                    upstream_output=context, pipeline_name="abc", log_manager=console_manager
                )
            )
            == "cat"
        )
        context_diff_version = OutputContext(
            step_key="foo",
            name="bar",
            mapping_key=None,
            log_manager=console_manager,
            metadata={},
            pipeline_name="fake",
            solid_def=None,
            dagster_type=Any,
            run_id=None,
            version="version2",
        )
        assert not store.has_output(context_diff_version)


def test_versioned_io_manager_with_resources():
    occurrence_log = []

    @io_manager(required_resource_keys={"foo"})
    def construct_memoizable_io_manager(_):
        class FakeIOManager(MemoizableIOManager):
            def handle_output(self, context, _obj):
                occurrence_log.append("handle")
                assert context.resources.foo == "bar"

            def load_input(self, context):
                occurrence_log.append("load")
                assert context.resources.foo == "bar"

            def has_output(self, context):
                occurrence_log.append("has")
                assert context.resources.foo == "bar"

        return FakeIOManager()

    @solid(version="baz")
    def basic_solid():
        pass

    @pipeline(
        mode_defs=[
            ModeDefinition(
                resource_defs={
                    "io_manager": construct_memoizable_io_manager,
                    "foo": ResourceDefinition.hardcoded_resource("bar"),
                }
            )
        ],
        tags={MEMOIZED_RUN_TAG: "true"},
    )
    def basic_pipeline():
        basic_solid()

    execute_pipeline(basic_pipeline)

    assert occurrence_log == ["has", "has", "handle"]
