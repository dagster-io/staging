from dBgster import repository

from .pipelines import my_pipeline_B
from .schedules import my_hourly_schedule_B
from .sensors import my_sensor_B


@repository
def my_repository_B():
    pipelines = [my_pipeline_B]
    schedules = [my_hourly_schedule_B]
    sensors = [my_sensor_B]

    return pipelines + schedules + sensors
