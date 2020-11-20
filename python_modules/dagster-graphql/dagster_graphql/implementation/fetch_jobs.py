from dagster import check
from dagster.core.definitions.job import JobType
from dagster.core.host_representation import (
    ExternalSchedule,
    ExternalSensor,
    JobSelector,
    RepositorySelector,
)
from graphql.execution.base import ResolveInfo

from .utils import UserFacingGraphQLError, capture_dauphin_error


@capture_dauphin_error
def get_job_definitions_or_error(graphene_info, repository_selector, job_type=None):
    check.inst_param(graphene_info, "graphene_info", ResolveInfo)
    check.inst_param(repository_selector, "repository_selector", RepositorySelector)
    check.opt_inst_param(job_type, "job_type", JobType)

    location = graphene_info.context.get_repository_location(repository_selector.location_name)
    repository = location.get_repository(repository_selector.repository_name)

    return graphene_info.schema.type_named("JobDefinitions")(
        results=[
            _get_dauphin_job_for_external_job(graphene_info, external_job)
            for external_job in repository.get_external_jobs()
        ]
    )


def _get_dauphin_job_for_external_job(graphene_info, external_job):
    if isinstance(external_job, ExternalSchedule):
        return graphene_info.schema.type_named("ScheduleDefinition")(
            graphene_info, external_job=external_job
        )
    if isinstance(external_job, ExternalSensor):
        return graphene_info.schema.type_named("SensorDefinition")(
            graphene_info, external_job=external_job
        )

    check.failed("Unknown job type received")


@capture_dauphin_error
def get_job_definition_or_error(graphene_info, job_selector):
    check.inst_param(graphene_info, "graphene_info", ResolveInfo)
    check.inst_param(job_selector, "job_selector", JobSelector)
    location = graphene_info.context.get_repository_location(job_selector.location_name)
    repository = location.get_repository(job_selector.repository_name)

    external_job = repository.get_external_job(job_selector.job_name)
    if not external_job:
        raise UserFacingGraphQLError(
            graphene_info.schema.type_named("JobDefinitionNotFoundError")(
                job_name=job_selector.job_name
            )
        )

    return _get_dauphin_job_for_external_job(graphene_info, external_job)
