import graphene
from dagster.core.storage.pipeline_run import PipelineRunStatus

GraphenePipelineRunStatus = graphene.Enum.from_enum(PipelineRunStatus)
