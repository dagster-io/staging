import graphene
from dagster.core.scheduler.job import JobTickStatus

from ..errors import PythonError
from ..pipelines.pipeline import PipelineRun


class ScheduleTickSuccessData(graphene.ObjectType):
    run = graphene.Field(PipelineRun)


class ScheduleTickFailureData(graphene.ObjectType):
    error = graphene.NonNull(PythonError)


def tick_specific_data_from_dagster_tick(graphene_info, tick):
    if tick.status == JobTickStatus.SUCCESS:
        if tick.run_ids and graphene_info.context.instance.has_run(tick.run_ids[0]):
            return ScheduleTickSuccessData(
                run=PipelineRun(graphene_info.context.instance.get_run_by_id(tick.run_ids[0]))
            )
        return ScheduleTickSuccessData(run=None)
    elif tick.status == JobTickStatus.FAILURE:
        error = tick.error
        return ScheduleTickFailureData(error=error)


class ScheduleTickSpecificData(graphene.Union):
    class Meta:
        types = (
            ScheduleTickSuccessData,
            ScheduleTickFailureData,
        )


class ScheduleTick(graphene.ObjectType):
    tick_id = graphene.NonNull(graphene.String)
    status = graphene.NonNull(JobTickStatus)
    timestamp = graphene.NonNull(graphene.Float)
    tick_specific_data = graphene.Field(ScheduleTickSpecificData)
