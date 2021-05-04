"""
In my staging environment, I want a sensor that kicks off my pipeline using my staging resources.

In my production environment, I want a sensor that kicks off my pipeline using my production
resources.
"""
from dagster import (
    solid,
    pipeline,
    repository,
    validate_sensor,
    create_sensor_context,
    ResourceDefinition,
)
from dagster import sensor, RunRequest


@solid(required_resource_keys={"my_resource"}, config_schema=int)
def my_solid():
    pass


@solid(required_resource_keys={"my_resource"})
def my_other_solid(_input1):
    pass


@pipeline
def my_pipeline():
    my_other_solid(my_solid())


@sensor(pipeline=my_pipeline)
def my_pipeline_sensor():
    yield RunRequest(None, run_config={"solids": {"my_solid": {"config": {7}}}})


my_pipeline_sensor_staging = my_pipeline_sensor.bind(
    {"my_resource": ResourceDefinition.hardcoded_resource(5)}, suffix="staging"
)


my_pipeline_sensor_prod = my_pipeline_sensor.bind(
    {"my_resource": ResourceDefinition.hardcoded_resource(5)}, suffix="prod"
)


@repository
def staging_repo():
    return [my_pipeline_sensor_staging]


@repository
def prod_repo():
    return [my_pipeline_sensor_prod]


def test_my_pipeline_sensor():
    """
    Make sure the sensor produces config that the pipeline accepts.
    """
    assert validate_sensor(my_pipeline_sensor_staging, create_sensor_context())
    assert validate_sensor(my_pipeline_sensor_prod, create_sensor_context())
