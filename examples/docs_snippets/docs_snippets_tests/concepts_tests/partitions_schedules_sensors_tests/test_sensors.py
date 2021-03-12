from contextlib import suppress

from dagster import SensorExecutionContext, execute_pipeline, pipeline, reconstructable, solid
from dagster.core.test_utils import instance_for_test
from docs_snippets.concepts.partitions_schedules_sensors.sensor_alert import (
    failure_alert_pipeline,
    pipeline_failure_sensor,
)


@solid(config_schema={"fail": bool})
def foo(context):
    if context.solid_config["fail"]:
        raise Exception("This will always fail!")


@pipeline
def your_pipeline_name():
    return foo()


def test_failure_alert_pipeline():
    result = execute_pipeline(failure_alert_pipeline, mode="test")
    assert result.success


def test_pipeline_failure_sensor_has_request():
    with instance_for_test() as instance, suppress(Exception):
        execute_pipeline(
            reconstructable(your_pipeline_name),
            run_config={"solids": {"foo": {"config": {"fail": True}}}},
            instance=instance,
        )

        context = SensorExecutionContext(
            instance_ref=instance.get_ref(), last_run_key=None, last_completion_time=None
        )

        requests = pipeline_failure_sensor.get_execution_data(context)
        assert len(requests) == 1


def test_pipeline_failure_sensor_has_no_request():
    with instance_for_test() as instance:
        execute_pipeline(
            reconstructable(your_pipeline_name),
            run_config={"solids": {"foo": {"config": {"fail": False}}}},
            instance=instance,
        )

        context = SensorExecutionContext(
            instance_ref=instance.get_ref(), last_run_key=None, last_completion_time=None
        )

        requests = pipeline_failure_sensor.get_execution_data(context)
        assert len(requests) == 0
