import graphene
from dagster import check
from dagster.core.definitions.events import AssetKey as DagsterAssetKey
from dagster.utils.error import SerializableErrorInfo

from ..implementation.utils import PipelineSelector
from .util import non_null_list


class Error(graphene.Interface):
    message = graphene.String(required=True)


class PythonError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    className = graphene.Field(graphene.String)
    stack = non_null_list(graphene.String)
    cause = graphene.Field(lambda: PythonError)

    def __init__(self, error_info):
        super().__init__()
        check.inst_param(error_info, "error_info", SerializableErrorInfo)
        self.message = error_info.message
        self.stack = error_info.stack
        self.cause = error_info.cause
        self.className = error_info.cls_name


class SchedulerNotDefinedError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    def __init__(self):
        super().__init__()
        self.message = "Scheduler is not defined for the currently loaded repository."


class PipelineSnapshotNotFoundError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    snapshot_id = graphene.NonNull(graphene.String)

    def __init__(self, snapshot_id):
        super().__init__()
        self.snapshot_id = check.str_param(snapshot_id, "snapshot_id")
        self.message = f"Pipeline snapshot {snapshot_id} is not present in the current instance."


class ReloadNotSupported(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    def __init__(self, location_name):
        super().__init__()
        self.message = f"Location {location_name} does not support reloading."


class RepositoryLocationNotFound(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    def __init__(self, location_name):
        super().__init__()
        self.message = f"Location {location_name} does not exist."


class PipelineNotFoundError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    pipeline_name = graphene.NonNull(graphene.String)
    repository_name = graphene.NonNull(graphene.String)
    repository_location_name = graphene.NonNull(graphene.String)

    def __init__(self, selector):
        super().__init__()
        check.inst_param(selector, "selector", PipelineSelector)
        self.pipeline_name = selector.pipeline_name
        self.repository_name = selector.repository_name
        self.repository_location_name = selector.location_name
        self.message = (
            "Could not find Pipeline "
            f"{selector.location_name}.{selector.repository_name}.{selector.pipeline_name}"
        )


class PipelineRunNotFoundError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    run_id = graphene.NonNull(graphene.String)

    def __init__(self, run_id):
        super().__init__()
        self.run_id = check.str_param(run_id, "run_id")
        self.message = f"Pipeline run {run_id} could not be found."


class InvalidPipelineRunsFilterError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    def __init__(self, message):
        super().__init__()
        self.message = check.str_param(message, "message")


class RunGroupNotFoundError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    run_id = graphene.NonNull(graphene.String)

    def __init__(self, run_id):
        super().__init__()
        self.run_id = check.str_param(run_id, "run_id")
        self.message = f"Run group of run {run_id} could not be found."


class PresetNotFoundError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    preset = graphene.NonNull(graphene.String)

    def __init__(self, preset, selector):
        super().__init__()
        self.preset = check.str_param(preset, "preset")
        self.message = f"Preset {preset} not found in pipeline {selector.pipeline_name}."


class ConflictingExecutionParamsError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    def __init__(self, conflicting_param):
        super().__init__()
        self.message = (
            f"Invalid ExecutionParams. Cannot define {conflicting_param} when using a preset."
        )


class ModeNotFoundError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    mode = graphene.NonNull(graphene.String)

    def __init__(self, mode, selector):
        super().__init__()
        self.mode = check.str_param(mode, "mode")
        self.message = f"Mode {mode} not found in pipeline {selector.pipeline_name}."


class InvalidStepError(graphene.ObjectType):
    invalid_step_key = graphene.NonNull(graphene.String)


class InvalidOutputError(graphene.ObjectType):
    step_key = graphene.NonNull(graphene.String)
    invalid_output_name = graphene.NonNull(graphene.String)


class PipelineRunConflict(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)


create_execution_params_error_types = (
    PresetNotFoundError,
    ConflictingExecutionParamsError,
)


class DagsterTypeNotFoundError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    dagster_type_name = graphene.NonNull(graphene.String)


class ScheduleNotFoundError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    schedule_name = graphene.NonNull(graphene.String)

    def __init__(self, schedule_name):
        super().__init__()
        self.schedule_name = check.str_param(schedule_name, "schedule_name")
        self.message = f"Schedule {self.schedule_name} could not be found."


class SensorNotFoundError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    sensor_name = graphene.NonNull(graphene.String)

    def __init__(self, sensor_name):
        super().__init__()
        self.name = check.str_param(sensor_name, "sensor_name")
        self.message = f"Sensor {sensor_name} is not present in the currently loaded repository."


class PartitionSetNotFoundError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    partition_set_name = graphene.NonNull(graphene.String)

    def __init__(self, partition_set_name):
        super().__init__()
        self.partition_set_name = check.str_param(partition_set_name, "partition_set_name")
        self.message = f"Partition set {self.partition_set_name} could not be found."


class RepositoryNotFoundError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    repository_name = graphene.NonNull(graphene.String)
    repository_location_name = graphene.NonNull(graphene.String)

    def __init__(self, repository_location_name, repository_name):
        super().__init__()
        self.repository_name = check.str_param(repository_name, "repository_name")
        self.repository_location_name = check.str_param(
            repository_location_name, "repository_location_name"
        )
        self.message = f"Could not find Repository {repository_location_name}.{repository_name}"


class AssetsNotSupportedError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)


class AssetNotFoundError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    def __init__(self, asset_key):
        super().__init__()
        self.asset_key = check.inst_param(asset_key, "asset_key", DagsterAssetKey)
        self.message = f"Asset key {asset_key.to_string()} not found."


types = [
    AssetNotFoundError,
    AssetsNotSupportedError,
    ConflictingExecutionParamsError,
    DagsterTypeNotFoundError,
    Error,
    InvalidOutputError,
    InvalidPipelineRunsFilterError,
    InvalidStepError,
    ModeNotFoundError,
    PartitionSetNotFoundError,
    PipelineNotFoundError,
    PipelineRunConflict,
    PipelineRunNotFoundError,
    PipelineSnapshotNotFoundError,
    PresetNotFoundError,
    PythonError,
    ReloadNotSupported,
    RepositoryLocationNotFound,
    RepositoryNotFoundError,
    RunGroupNotFoundError,
    ScheduleNotFoundError,
    SchedulerNotDefinedError,
    SensorNotFoundError,
]
