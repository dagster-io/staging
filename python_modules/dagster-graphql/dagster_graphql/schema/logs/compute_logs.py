import graphene
from dagster import check
from dagster.core.storage.compute_log_manager import ComputeLogFileData


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
