import graphene
from dagster.core.storage.pipeline_run import PipelineRunStatus as DagsterPipelineRunStatus

PipelineRunStatus = graphene.Enum.from_enum(DagsterPipelineRunStatus)
