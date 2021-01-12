from typing import NamedTuple

from dagster.core.definitions.resource import ResourceDefinition
from dagster.core.storage.file_manager import FileHandle, FileManager, check_file_like_obj
from dagster.core.storage.io_manager import IOManager, IOManagerDefinition, io_manager


class FileHandleWithFileManager(NamedTuple):
    file_manager: FileManager
    file_handle: FileHandle

    def read_data(self):
        return self.file_manager.read_data(self.file_handle)

    def read(self, mode="rb"):
        return self.file_manager.read(self.file_handle, mode=mode)


class BytesIOManager(IOManager):
    def __init__(self, file_manager):
        self._file_manager = file_manager

    def _get_file_key(self, context) -> str:
        return "__".join(context.get_run_scoped_output_identifier())

    def handle_output(self, context, obj):
        if isinstance(obj, bytes):
            self._file_manager.write_data(obj, file_key=self._get_file_key(context))
        elif check_file_like_obj(obj):
            self._file_manager.write(obj, file_key=self._get_file_key(context))

    def load_input(self, context):
        file_handle = self._file_manager.get_file_handle(
            self._get_file_key(context.upstream_output), ext=None
        )
        return FileHandleWithFileManager(self._file_manager, file_handle)


def bytes_io_manager(file_manager_def: ResourceDefinition) -> IOManagerDefinition:
    @io_manager(config_schema=file_manager_def.config_schema)
    def _io_manager(init_context):
        file_manager = file_manager_def.resource_fn(init_context)
        return BytesIOManager(file_manager)

    return _io_manager
