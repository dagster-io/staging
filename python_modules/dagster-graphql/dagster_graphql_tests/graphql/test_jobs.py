from dagster.daemon import get_default_daemon_logger
from dagster.scheduler.sensor import execute_sensor_iteration
from dagster_graphql.test.utils import (
    dict_repo_location_name,
    dict_repo_name,
    execute_dagster_graphql,
    infer_job_selector,
    main_repo_location_name,
    main_repo_name,
)

from .graphql_context_test_suite import (
    GraphQLContextVariant,
    get_dict_recon_repo,
    make_graphql_context_test_suite,
)

GET_JOB_QUERY = """
query JobQuery($jobSelector: JobSelector!) {
  jobStateOrError(jobSelector: $jobSelector) {
    __typename
    ... on PythonError {
      message
      stack
    }
    ... on JobState {
        id
        nextTick {
            timestamp
        }
    }
  }
}
"""


def _create_sensor_tick(instance):
    list(execute_sensor_iteration(instance, get_default_daemon_logger("SensorDaemon")))


class TestDictBasedRepository(
    make_graphql_context_test_suite(
        context_variants=GraphQLContextVariant.all_multi_location_variants(),
        recon_repo=get_dict_recon_repo(),
    )
):
    def test_schedule_next_tick(self, graphql_context):
        external_repository = graphql_context.get_repository_location(
            dict_repo_location_name()
        ).get_repository(dict_repo_name())
        graphql_context.instance.reconcile_scheduler_state(external_repository)

        schedule_name = "no_config_pipeline_hourly_schedule"
        external_schedule = external_repository.get_external_schedule(schedule_name)
        job_selector = {
            "repositoryLocationName": dict_repo_location_name(),
            "repositoryName": dict_repo_name(),
            "jobName": schedule_name,
        }

        # need to be running in order to generate a future tick
        graphql_context.instance.start_schedule_and_update_storage_state(external_schedule)
        result = execute_dagster_graphql(
            graphql_context, GET_JOB_QUERY, variables={"jobSelector": job_selector}
        )

        assert result.data
        assert result.data["jobStateOrError"]["__typename"] == "JobState"
        next_tick = result.data["jobStateOrError"]["nextTick"]
        assert next_tick

    def test_sensor_next_tick(self, graphql_context):
        external_repository = graphql_context.get_repository_location(
            dict_repo_location_name()
        ).get_repository(dict_repo_name())
        graphql_context.instance.reconcile_scheduler_state(external_repository)

        sensor_name = "always_no_config_sensor"
        external_sensor = external_repository.get_external_sensor(sensor_name)
        job_selector = {
            "repositoryLocationName": dict_repo_location_name(),
            "repositoryName": dict_repo_name(),
            "jobName": sensor_name,
        }

        # need to be running and create a sensor tick in the last 30 seconds in order to generate a
        # future tick
        graphql_context.instance.start_sensor(external_sensor)
        _create_sensor_tick(graphql_context.instance)

        result = execute_dagster_graphql(
            graphql_context, GET_JOB_QUERY, variables={"jobSelector": job_selector}
        )

        assert result.data
        assert result.data["jobStateOrError"]["__typename"] == "JobState"
        next_tick = result.data["jobStateOrError"]["nextTick"]
        assert next_tick


def test_schedule_next_tick(graphql_context):
    external_repository = graphql_context.get_repository_location(
        main_repo_location_name()
    ).get_repository(main_repo_name())
    graphql_context.instance.reconcile_scheduler_state(external_repository)

    schedule_name = "no_config_pipeline_hourly_schedule"
    external_schedule = external_repository.get_external_schedule(schedule_name)
    job_selector = infer_job_selector(graphql_context, schedule_name)

    # need to be running in order to generate a future tick
    graphql_context.instance.start_schedule_and_update_storage_state(external_schedule)
    result = execute_dagster_graphql(
        graphql_context, GET_JOB_QUERY, variables={"jobSelector": job_selector}
    )

    assert result.data
    assert result.data["jobStateOrError"]["__typename"] == "JobState"
    next_tick = result.data["jobStateOrError"]["nextTick"]
    assert next_tick


def test_sensor_next_tick(graphql_context):
    external_repository = graphql_context.get_repository_location(
        main_repo_location_name()
    ).get_repository(main_repo_name())
    graphql_context.instance.reconcile_scheduler_state(external_repository)

    sensor_name = "always_no_config_sensor"
    external_sensor = external_repository.get_external_sensor(sensor_name)
    job_selector = infer_job_selector(graphql_context, sensor_name)

    # need to be running and create a sensor tick in the last 30 seconds in order to generate a
    # future tick
    graphql_context.instance.start_sensor(external_sensor)
    _create_sensor_tick(graphql_context.instance)

    result = execute_dagster_graphql(
        graphql_context, GET_JOB_QUERY, variables={"jobSelector": job_selector}
    )

    assert result.data
    assert result.data["jobStateOrError"]["__typename"] == "JobState"
    next_tick = result.data["jobStateOrError"]["nextTick"]
    assert next_tick
