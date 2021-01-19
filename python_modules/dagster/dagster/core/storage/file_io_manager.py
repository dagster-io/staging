from contextlib import contextmanager
from typing import NamedTuple

from dagster.core.storage.file_manager import FileHandle, FileManager
from dagster.core.storage.io_manager import IOManager


class Readable(NamedTuple):
    file_manager: FileManager
    file_handle: FileHandle

    def read_data(self):
        return self.file_manager.read_data(self.file_handle)

    @contextmanager
    def read(self, mode="rb"):
        with self.file_manager.read(self.file_handle, mode=mode) as file_obj:
            return file_obj


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
