from dagster import repository

from .pipelines import my_pipeline
from .schedules import my_hourly_schedule
from .sensors import my_sensor


@repository
def my_repository():
    pipelines = [my_pipeline]
    schedules = [my_hourly_schedule]
    sensors = [my_sensor]

    return pipelines + schedules + sensors
