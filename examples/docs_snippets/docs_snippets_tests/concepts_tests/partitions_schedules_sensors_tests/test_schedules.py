import pytest
from dagster import ModeDefinition, build_schedule_context, pipeline, solid, validate_run_config
from dagster.core.test_utils import instance_for_test
from docs_snippets.concepts.partitions_schedules_sensors.schedules.schedule_examples import (
    my_daily_schedule,
    my_hourly_schedule,
    my_modified_preset_schedule,
    my_monthly_schedule,
    my_preset_schedule,
    my_weekly_schedule,
    test_hourly_schedule,
)


def test_schedule_testing_example():
    test_hourly_schedule()


@pytest.mark.parametrize(
    "schedule_to_test",
    [
        my_hourly_schedule,
        my_daily_schedule,
        my_weekly_schedule,
        my_monthly_schedule,
        my_preset_schedule,
        my_modified_preset_schedule,
    ],
)
def test_schedule_examples(schedule_to_test):
    @solid(config_schema={"date": str})
    def process_data_for_date(_):
        pass

    @pipeline(mode_defs=[ModeDefinition("basic")])
    def pipeline_for_test():
        process_data_for_date()

    with instance_for_test() as instance:
        for run_request in schedule_to_test.get_execution_data(
            build_schedule_context(instance=instance)
        ):
            assert validate_run_config(pipeline_for_test, run_request.run_config)
