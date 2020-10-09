from dagster import (
    InputDefinition,
    Int,
    OutputDefinition,
    daily_schedule,
    lambda_solid,
    pipeline,
    repository,
)
from dagster.seven import get_current_datetime_in_utc


@lambda_solid(input_defs=[InputDefinition("num", Int)], output_def=OutputDefinition(Int))
def add_one(num):
    return num + 1


@lambda_solid(input_defs=[InputDefinition("num", Int)], output_def=OutputDefinition(Int))
def mult_two(num):
    return num * 2


@pipeline
def math():
    return mult_two(num=add_one())


@daily_schedule(pipeline_name="math", start_date=get_current_datetime_in_utc())
def my_schedule(_):
    return {"solids": {"mult_two": {"inputs": {"num": {"value": 2}}}}}


@repository
def test_repository():
    return [math]
