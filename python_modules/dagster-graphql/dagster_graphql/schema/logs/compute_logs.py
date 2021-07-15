import graphene
from dagster import check
from dagster.core.storage.compute_log_manager import ComputeIOType, ComputeLogFileData
from dagster.core.storage.captured_log_manager import (
    CapturedLogData,
    CapturedLogManager,
    CapturedLogMetadata,
)
from typing import Optional, Tuple

MAX_BYTES_CHUNK_READ = 4194304  # 4 MB


class GrapheneComputeIOType(graphene.Enum):
    STDOUT = "stdout"
    STDERR = "stderr"

    class Meta:
        name = "ComputeIOType"


class GrapheneComputeLogFile(graphene.ObjectType):
    class Meta:
        name = "ComputeLogFile"

    path = graphene.NonNull(graphene.String)
    data = graphene.Field(
        graphene.String, description="The data output captured from step computation at query time"
    )
    cursor = graphene.NonNull(graphene.Int)
    size = graphene.NonNull(graphene.Int)
    download_url = graphene.Field(graphene.String)


# sets up a callback for an observable to watch the captured log updates for a given log_key
def captured_log_callback(
    captured_log_manager: CapturedLogManager,
    log_key: str,
    namespace: Optional[str],
    io_type: ComputeIOType,
    cursor: Optional[str],
):
    def _callback(observer):
        should_fetch = True
        current_cursor = cursor
        metadata = None
        while should_fetch:
            if io_type == ComputeIOType.STDERR:
                log_data = captured_log_manager.get_stderr(
                    log_key, namespace, current_cursor, max_bytes=MAX_BYTES_CHUNK_READ
                )
                if not metadata:
                    metadata = captured_log_manager.get_stderr_metadata(log_key, namespace)
            else:
                log_data = captured_log_manager.get_stdout(
                    log_key, namespace, current_cursor, max_bytes=MAX_BYTES_CHUNK_READ
                )
                if not metadata:
                    metadata = captured_log_manager.get_stdout_metadata(log_key, namespace)

            observer.on_next((log_data, metadata))
            should_fetch = log_data.data and len(log_data.data) >= MAX_BYTES_CHUNK_READ
            current_cursor = log_data.cursor

        if captured_log_manager.is_capture_complete(log_key, namespace):
            observer.on_completed()

    return _callback


# Graphene layer that converts captured log updates (from CapturedLogManager) to the appropriate
# Graphene objects
def captured_log_update_to_graphene(update: Tuple[CapturedLogData, CapturedLogMetadata]):
    log_data, metadata = update
    return GrapheneComputeLogFile(
        path=metadata.location,
        data=log_data.chunk.decode("utf-8"),
        cursor=log_data.cursor,
        size=len(log_data.chunk),
        download_url=metadata.download_url,
    )


# Legacy graphene layer that converts compute log updates (from ComputeLogManager) to the
# appropriate Graphene objects
def from_compute_log_file(_graphene_info, file):
    check.opt_inst_param(file, "file", ComputeLogFileData)
    if not file:
        return None
    return GrapheneComputeLogFile(
        path=file.path,
        data=file.data,
        cursor=file.cursor,
        size=file.size,
        download_url=file.download_url,
    )


class GrapheneComputeLogs(graphene.ObjectType):
    runId = graphene.NonNull(graphene.String)
    stepKey = graphene.NonNull(graphene.String)
    stdout = graphene.Field(GrapheneComputeLogFile)
    stderr = graphene.Field(GrapheneComputeLogFile)

    class Meta:
        name = "ComputeLogs"

    def _resolve_compute_log(self, graphene_info, io_type):
        return graphene_info.context.instance.compute_log_manager.read_logs_file(
            self.runId, self.stepKey, io_type, 0
        )

    def resolve_stdout(self, graphene_info):
        return self._resolve_compute_log(graphene_info, ComputeIOType.STDOUT)

    def resolve_stderr(self, graphene_info):
        return self._resolve_compute_log(graphene_info, ComputeIOType.STDERR)
