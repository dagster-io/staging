import graphene

from ..implementation.utils import ExecutionMetadata
from .pipelines.status import PipelineRunStatus
from .runs import RunConfigData
from .util import non_null_list


class AssetKeyInput(graphene.InputObjectType):
    path = non_null_list(graphene.String)


class ExecutionTag(graphene.InputObjectType):
    key = graphene.NonNull(graphene.String)
    value = graphene.NonNull(graphene.String)


class PipelineRunsFilter(graphene.InputObjectType):
    class Meta:
        description = """This type represents a filter on pipeline runs.
        Currently, you may only pass one of the filter options."""

    runIds = graphene.List(graphene.String)
    pipelineName = graphene.Field(graphene.String)
    tags = graphene.List(graphene.NonNull(ExecutionTag))
    statuses = graphene.List(graphene.NonNull(PipelineRunStatus))
    snapshotId = graphene.Field(graphene.String)

    def to_selector(self):
        if self.tags:
            # We are wrapping self.tags in a list because graphene.List is not marked as iterable
            tags = {tag["key"]: tag["value"] for tag in list(self.tags)}
        else:
            tags = None

        if self.statuses:
            statuses = [
                PipelineRunStatus[status]
                for status in self.statuses  # pylint: disable=not-an-iterable
            ]
        else:
            statuses = None

        return PipelineRunsFilter(
            run_ids=self.runIds,
            pipeline_name=self.pipelineName,
            tags=tags,
            statuses=statuses,
            snapshot_id=self.snapshotId,
        )


class StepOutputHandle(graphene.InputObjectType):
    stepKey = graphene.NonNull(graphene.String)
    outputName = graphene.NonNull(graphene.String)


class PipelineSelector(graphene.InputObjectType):
    class Meta:
        description = """This type represents the fields necessary to identify a
        pipeline or pipeline subset."""

    pipelineName = graphene.NonNull(graphene.String)
    repositoryName = graphene.NonNull(graphene.String)
    repositoryLocationName = graphene.NonNull(graphene.String)
    solidSelection = graphene.List(graphene.NonNull(graphene.String))


class RepositorySelector(graphene.InputObjectType):
    class Meta:
        description = """This type represents the fields necessary to identify a repository."""

    repositoryName = graphene.NonNull(graphene.String)
    repositoryLocationName = graphene.NonNull(graphene.String)


class PartitionSetSelector(graphene.InputObjectType):
    class Meta:
        description = """This type represents the fields necessary to identify a
        pipeline or pipeline subset."""

    partitionSetName = graphene.NonNull(graphene.String)
    repositorySelector = graphene.NonNull(RepositorySelector)


class PartitionBackfillParams(graphene.InputObjectType):
    selector = graphene.NonNull(PartitionSetSelector)
    partitionNames = non_null_list(graphene.String)
    reexecutionSteps = graphene.List(graphene.NonNull(graphene.String))
    fromFailure = graphene.Boolean()
    tags = graphene.List(graphene.NonNull(ExecutionTag))


class SensorSelector(graphene.InputObjectType):
    class Meta:
        description = """This type represents the fields necessary to identify a sensor."""

    repositoryName = graphene.NonNull(graphene.String)
    repositoryLocationName = graphene.NonNull(graphene.String)
    sensorName = graphene.NonNull(graphene.String)


class ScheduleSelector(graphene.InputObjectType):
    class Meta:
        description = """This type represents the fields necessary to identify a schedule."""

    repositoryName = graphene.NonNull(graphene.String)
    repositoryLocationName = graphene.NonNull(graphene.String)
    scheduleName = graphene.NonNull(graphene.String)


class ExecutionParams(graphene.InputObjectType):
    selector = graphene.NonNull(
        PipelineSelector,
        description="""Defines the pipeline and solid subset that should be executed.
        All subsequent executions in the same run group (for example, a single-step
        re-execution) are scoped to the original run's pipeline selector and solid
        subset.""",
    )
    runConfigData = graphene.Field(RunConfigData)
    mode = graphene.Field(graphene.String)
    executionMetadata = graphene.Field(
        ExecutionMetadata,
        description="""Defines run tags and parent / root relationships.\n\nNote: To
        'restart from failure', provide a `parentRunId` and pass the
        'dagster/is_resume_retry' tag. Dagster's automatic step key selection will
        override any stepKeys provided.""",
    )
    stepKeys = graphene.Field(
        graphene.List(graphene.NonNull(graphene.String)),
        description="""Defines step keys to execute within the execution plan defined
        by the pipeline `selector`. To execute the entire execution plan, you can omit
        this parameter, provide an empty array, or provide every step name.""",
    )
    preset = graphene.Field(graphene.String)


class MarshalledInput(graphene.InputObjectType):
    input_name = graphene.NonNull(graphene.String)
    key = graphene.NonNull(graphene.String)


class MarshalledOutput(graphene.InputObjectType):
    output_name = graphene.NonNull(graphene.String)
    key = graphene.NonNull(graphene.String)


class StepExecution(graphene.InputObjectType):
    stepKey = graphene.NonNull(graphene.String)
    marshalledInputs = graphene.List(graphene.NonNull(MarshalledInput))
    marshalledOutputs = graphene.List(graphene.NonNull(MarshalledOutput))


class ExecutionMetadata(graphene.InputObjectType):
    runId = graphene.String()
    tags = graphene.List(graphene.NonNull(ExecutionTag))
    rootRunId = graphene.String(
        description="""The ID of the run at the root of the run group. All partial /
        full re-executions should use the first run as the rootRunID so they are
        grouped together."""
    )
    parentRunId = graphene.String(
        description="""The ID of the run serving as the parent within the run group.
        For the first re-execution, this will be the same as the `rootRunId`. For
        subsequent runs, the root or a previous re-execution could be the parent run."""
    )


class RetriesPreviousAttempts(graphene.InputObjectType):
    key = graphene.String()
    count = graphene.Int()


class Retries(graphene.InputObjectType):
    mode = graphene.Field(graphene.String)
    retries_previous_attempts = graphene.List(RetriesPreviousAttempts)


def create_retries_params(retries_config):
    return Retries.from_graphql_input(retries_config)
