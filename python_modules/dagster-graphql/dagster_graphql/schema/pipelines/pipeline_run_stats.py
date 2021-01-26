import graphene
from dagster import check

from ..errors import PythonError


class PipelineRunStatsSnapshot(graphene.ObjectType):
    id = graphene.NonNull(graphene.String)
    runId = graphene.NonNull(graphene.String)
    stepsSucceeded = graphene.NonNull(graphene.Int)
    stepsFailed = graphene.NonNull(graphene.Int)
    materializations = graphene.NonNull(graphene.Int)
    expectations = graphene.NonNull(graphene.Int)
    startTime = graphene.Field(graphene.Float)
    endTime = graphene.Field(graphene.Float)

    def __init__(self, stats):
        super().__init__(
            id="stats-" + stats.run_id,
            runId=stats.run_id,
            stepsSucceeded=stats.steps_succeeded,
            stepsFailed=stats.steps_failed,
            materializations=stats.materializations,
            expectations=stats.expectations,
            startTime=stats.start_time,
            endTime=stats.end_time,
        )
        self._stats = check.inst_param(stats, "stats", PipelineRunStatsSnapshot)


class PipelineRunStatsOrError(graphene.Union):
    class Meta:
        types = (PipelineRunStatsSnapshot, PythonError)
