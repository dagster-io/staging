from dagster import repository

from .pipelines import my_pipeline_A
from .schedules import my_hourly_schedule_A
from .sensors import my_sensor_A


@repository
def my_repository_A():
    pipelines = [my_pipeline_A]
    schedules = [my_hourly_schedule_A]
    sensors = [my_sensor_A]

    return pipelines + schedules + sensors
