import io
import os
import shutil
import uuid
from contextlib import contextmanager

from dagster import check
from dagster.config import Field
from dagster.config.source import StringSource
from dagster.core.definitions.resource import resource
from dagster.core.instance import DagsterInstance
from dagster.core.storage.file_io_manager import FileIOManager
from dagster.core.storage.file_manager import FileHandle, FileManager, check_file_like_obj
from dagster.core.storage.io_manager import io_manager
from dagster.core.types.decorator import usable_as_dagster_type
from dagster.utils import mkdir_p

from .temp_file_manager import TempfileManager


@usable_as_dagster_type
class LocalFileHandle(FileHandle):
    def __init__(self, path):
        self._path = check.str_param(path, "path")

    @property
    def path(self):
        return self._path

    @property
    def path_desc(self):
        return self._path


@resource(config_schema={"base_dir": Field(StringSource, default_value=".", is_required=False)})
def local_file_manager(init_context):
    return LocalFileManager(init_context.resource_config["base_dir"])


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


class LocalFileManager(FileManager):
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self._base_dir_ensured = False
        self._temp_file_manager = TempfileManager()

    @staticmethod
    def for_instance(instance, run_id):
        check.inst_param(instance, "instance", DagsterInstance)
        return LocalFileManager(instance.file_manager_directory(run_id))

    def ensure_base_dir_exists(self):
        if self._base_dir_ensured:
            return

        mkdir_p(self.base_dir)

        self._base_dir_ensured = True

    def copy_handle_to_local_temp(self, file_handle):
        check.inst_param(file_handle, "file_handle", FileHandle)
        with self.read(file_handle, "rb") as handle_obj:
            temp_file_obj = self._temp_file_manager.tempfile()
            temp_file_obj.write(handle_obj.read())
            temp_name = temp_file_obj.name
            temp_file_obj.close()
            return temp_name

    @contextmanager
    def read(self, file_handle, mode="rb"):
        check.inst_param(file_handle, "file_handle", LocalFileHandle)
        check.str_param(mode, "mode")
        check.param_invariant(mode in {"r", "rb"}, "mode")

        with open(file_handle.path, mode) as file_obj:
            yield file_obj

    def read_data(self, file_handle):
        with self.read(file_handle, mode="rb") as file_obj:
            return file_obj.read()

    def write_data(self, data, ext=None, file_key: str = None):
        check.inst_param(data, "data", bytes)
        return self.write(io.BytesIO(data), mode="wb", ext=ext, file_key=file_key)

    def write(self, file_obj, mode="wb", ext=None, file_key: str = None):
        check_file_like_obj(file_obj)
        check.opt_str_param(ext, "ext")

        self.ensure_base_dir_exists()

        file_key = file_key if file_key else str(uuid.uuid4())
        file_handle = self.get_file_handle(file_key=file_key, ext=ext)
        with open(file_handle.path, mode) as dest_file_obj:
            shutil.copyfileobj(file_obj, dest_file_obj)
            return file_handle

    def get_file_handle(self, file_key: str, ext: str) -> LocalFileHandle:
        dest_file_path = os.path.join(
            self.base_dir, file_key + (("." + ext) if ext is not None else "")
        )
        return LocalFileHandle(dest_file_path)

    def delete_local_temp(self):
        self._temp_file_manager.close()
