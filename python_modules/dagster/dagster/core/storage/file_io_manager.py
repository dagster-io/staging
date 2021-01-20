from contextlib import contextmanager
from typing import NamedTuple

from dagster import check
from dagster.config import Field
from dagster.config.source import StringSource
from dagster.core.storage.file_manager import FileHandle, FileManager, LocalFileManager
from dagster.core.storage.io_manager import IOManager, io_manager


class Readable(NamedTuple):
    file_manager: FileManager
    file_handle: FileHandle

    def read_data(self):
        return self.file_manager.read_data(self.file_handle)

    @contextmanager
    def read(self, mode="rb"):
        check.invariant(mode in ["r", "rb"], 'Mode must be "r" or "rb"')
        with self.file_manager.read(self.file_handle, mode=mode) as file_obj:
            yield file_obj


class FileIOManager(IOManager):
    def __init__(self, file_manager):
        self._file_manager = file_manager

    def _get_file_key(self, context) -> str:
        return "__".join(context.get_run_scoped_output_identifier())

    def handle_output(self, context, obj):
        ext = context.metadata.get("extension")
        file_key = self._get_file_key(context)

        if isinstance(obj, bytes):
            file_handle = self._file_manager.write_data(obj, file_key=file_key, ext=ext)
        elif obj and hasattr(obj, "read"):
            file_handle = self._file_manager.write(obj, file_key=file_key, ext=ext)
            obj.close()
        else:
            raise TypeError(
                "Output to handle must be a bytes object or a file-like object, "
                f"but instead was of type {type(obj)}."
            )

        context.log.info(f"Wrote output to {file_handle.path_desc}")

    def load_input(self, context):
        file_handle = self._file_manager.get_file_handle(
            self._get_file_key(context.upstream_output), ext=None
        )
        return Readable(self._file_manager, file_handle)


@io_manager(config_schema={"base_dir": Field(StringSource, default_value=".", is_required=False)})
def local_file_io_manager(init_context):
    """An :py:class:`IOManager` that accepts outputs that are streams or chunks of bytes and stores
    them as files on the local machine.

    The handled outputs must be either:
    * The Python bytes primitive type.
    * Binary I/O file objects.

    The object returned to downstream solids is a :py:class:`Readable`, which has methods to
    retrieve the bytes as file objects or as bytes.
    """
    return FileIOManager(LocalFileManager(init_context.resource_config["base_dir"]))
