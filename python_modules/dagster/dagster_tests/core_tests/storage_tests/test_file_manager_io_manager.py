from dagster import (
    ModeDefinition,
    OutputDefinition,
    execute_pipeline,
    local_file_manager,
    pipeline,
    solid,
)
from dagster.core.storage.file_manager_io_manager import bytes_io_manager


def test_local():
    foo_bytes = "foo".encode()

    @solid(output_defs=[OutputDefinition(manager_key="bytes")])
    def solid1(_):
        return foo_bytes

    @solid
    def solid2(_, input_handle):
        assert input_handle.read_data() == foo_bytes

    @pipeline(
        mode_defs=[ModeDefinition(resource_defs={"bytes": bytes_io_manager(local_file_manager)})]
    )
    def my_pipeline():
        solid2(solid1())

    execute_pipeline(my_pipeline)
