import graphene
from dagster import check

from ..errors import PipelineNotFoundError, PipelineSnapshotNotFoundError, PythonError
from .pipeline import IPipelineSnapshot, IPipelineSnapshotMixin
from .pipeline_ref import PipelineReference


class PipelineSnapshot(IPipelineSnapshotMixin, graphene.ObjectType):
    def __init__(self, represented_pipeline):
        from dagster.core.host_representation import RepresentedPipeline

        super().__init__()
        self._represented_pipeline = check.inst_param(
            represented_pipeline, "represented_pipeline", RepresentedPipeline
        )

    class Meta:
        interfaces = (IPipelineSnapshot, PipelineReference)

    def get_represented_pipeline(self):
        return self._represented_pipeline


class PipelineSnapshotOrError(graphene.Union):
    class Meta:
        types = (
            PipelineNotFoundError,
            PipelineSnapshot,
            PipelineSnapshotNotFoundError,
            PythonError,
        )
