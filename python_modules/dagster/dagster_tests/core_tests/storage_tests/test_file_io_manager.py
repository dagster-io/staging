import io
from tempfile import TemporaryDirectory

import pytest
from dagster import ModeDefinition, OutputDefinition, execute_pipeline, pipeline, solid
from dagster.core.storage.file_io_manager import local_file_io_manager


def test_bytes():
    with TemporaryDirectory() as tempdir:
        foo_bytes = "foo".encode()

        @solid(output_defs=[OutputDefinition(io_manager_key="bytes")])
        def solid1(_):
            return foo_bytes

        @solid
        def solid2(_, input_handle):
            assert input_handle.read_data() == foo_bytes

        @pipeline(
            mode_defs=[
                ModeDefinition(
                    resource_defs={"bytes": local_file_io_manager.configured({"base_dir": tempdir})}
                )
            ]
        )
        def my_pipeline():
            solid2(solid1())

        execute_pipeline(my_pipeline)


def test_bytes_read():
    with TemporaryDirectory() as tempdir:
        foo_bytes = "foo".encode()

        @solid(output_defs=[OutputDefinition(io_manager_key="bytes")])
        def solid1(_):
            return foo_bytes

        @solid
        def solid2(_, input_handle):
            with input_handle.read() as f:
                assert f.read() == foo_bytes

        @pipeline(
            mode_defs=[
                ModeDefinition(
                    resource_defs={"bytes": local_file_io_manager.configured({"base_dir": tempdir})}
                )
            ]
        )
        def my_pipeline():
            solid2(solid1())

        execute_pipeline(my_pipeline)


def test_binary_file_obj():
    with TemporaryDirectory() as tempdir:

        @solid(output_defs=[OutputDefinition(io_manager_key="bytes")])
        def solid1(_):
            return io.BytesIO("foo".encode())

        @solid
        def solid2(_, input_handle):
            assert input_handle.read_data() == "foo".encode()

        @pipeline(
            mode_defs=[
                ModeDefinition(
                    resource_defs={"bytes": local_file_io_manager.configured({"base_dir": tempdir})}
                )
            ]
        )
        def my_pipeline():
            solid2(solid1())

        execute_pipeline(my_pipeline)


def test_string_file_obj():
    with TemporaryDirectory() as tempdir:

        @solid(output_defs=[OutputDefinition(io_manager_key="bytes")])
        def solid1(_):
            return io.StringIO("foo")

        @pipeline(
            mode_defs=[
                ModeDefinition(
                    resource_defs={"bytes": local_file_io_manager.configured({"base_dir": tempdir})}
                )
            ]
        )
        def my_pipeline():
            solid1()

        with pytest.raises(TypeError):
            execute_pipeline(my_pipeline)
