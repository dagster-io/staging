from dagster.config import Field
from dagster.config.source import StringSource
from dagster.core.definitions.executor import ExecutionBoundaryType
from dagster.core.errors import DagsterUnmetExecutorRequirementsError
from dagster.core.execution.context.init import InitResourceContext
from dagster.core.storage.io_manager import IOManager, io_manager

from .fs_io_manager import PickledObjectFilesystemIOManager
from .mem_io_manager import InMemoryIOManager


class DefaultIOManager(IOManager):
    def __init__(self, base_dir: str):
        self._mem_io_manager = InMemoryIOManager()
        self._fs_io_manager = PickledObjectFilesystemIOManager(base_dir=base_dir)

    def handle_output(self, context, obj):
        if context.execution_boundary_type == ExecutionBoundaryType.SHARED_PROCESS:
            self._mem_io_manager.handle_output(context, obj)
        elif context.execution_boundary_type == ExecutionBoundaryType.SHARED_LOCAL_FILESYSTEM:
            self._fs_io_manager.handle_output(context, obj)
        else:
            raise DagsterUnmetExecutorRequirementsError(
                "The default IO manager stores outputs in-memory or on the local filesystem, but "
                "the pipeline is being executed by an executor that does not execute steps in the "
                "same process or with a shared local filesystem."
            )

    def load_input(self, context):
        if context.upstream_output.execution_boundary_type == ExecutionBoundaryType.SHARED_PROCESS:
            return self._mem_io_manager.load_input(context)
        elif (
            context.upstream_output.execution_boundary_type
            == ExecutionBoundaryType.SHARED_LOCAL_FILESYSTEM
        ):
            return self._fs_io_manager.load_input(context)
        else:
            raise DagsterUnmetExecutorRequirementsError(
                "The default IO manager stores outputs in-memory or on the local filesystem, but "
                "the pipeline is being executed by an executor that does not execute steps in the "
                "same process or with a shared local filesystem."
            )


@io_manager(config_schema={"base_dir": Field(StringSource, is_required=False)})
def default_io_manager(init_context: InitResourceContext) -> DefaultIOManager:
    """Built-in IO manager that stores outputs on the local filesystem or in-memory depending on the
    executor.

    If the executor places steps in the same process, then outputs will be stored in-memory. If the
    executor places steps on the same machine, then outputs will be pickled and stored on the local
    filesystem.

    Allows users to specify a base directory where all the step outputs will be stored. By
    default, step outputs will be stored in the directory specified by local_artifact_storage in
    your dagster.yaml file (which will be a temporary directory if not explicitly set).

    Serializes and deserializes output values using pickling and automatically constructs
    the filepaths for the assets.
    """
    base_dir = init_context.resource_config.get(
        "base_dir", init_context.instance.storage_directory()
    )

    return DefaultIOManager(base_dir)
