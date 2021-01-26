import graphene
from dagster import check
from dagster.core.storage.compute_log_manager import ComputeIOType as DagsterComputeIOType
from dagster.core.storage.compute_log_manager import ComputeLogFileData

ComputeIOType = graphene.Enum.from_enum(DagsterComputeIOType)


class ComputeLogFile(graphene.ObjectType):
    path = graphene.NonNull(graphene.String)
    data = graphene.Field(
        graphene.String, description="The data output captured from step computation at query time"
    )
    cursor = graphene.NonNull(graphene.Int)
    size = graphene.NonNull(graphene.Int)
    download_url = graphene.Field(graphene.String)


def from_compute_log_file(_graphene_info, file):
    check.opt_inst_param(file, "file", ComputeLogFileData)
    if not file:
        return None
    return ComputeLogFile(
        path=file.path,
        data=file.data,
        cursor=file.cursor,
        size=file.size,
        download_url=file.download_url,
    )


class ComputeLogs(graphene.ObjectType):
    runId = graphene.NonNull(graphene.String)
    stepKey = graphene.NonNull(graphene.String)
    stdout = graphene.Field(ComputeLogFile)
    stderr = graphene.Field(ComputeLogFile)

    def _resolve_compute_log(self, graphene_info, io_type):
        return graphene_info.context.instance.compute_log_manager.read_logs_file(
            self.runId, self.stepKey, io_type, 0
        )

    def resolve_stdout(self, graphene_info):
        return self._resolve_compute_log(graphene_info, DagsterComputeIOType.STDOUT)

    def resolve_stderr(self, graphene_info):
        return self._resolve_compute_log(graphene_info, DagsterComputeIOType.STDERR)
