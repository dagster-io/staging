# pylint: disable=unused-argument

import datetime

from dagster import InputDefinition, daily_schedule, pipeline, repository, solid


@solid
def return_one(_):
    return 1


@solid
def return_two(_):
    return 2


@solid(input_defs=[InputDefinition("left"), InputDefinition("right")])
def add(_, left, right):
    return left + right


@solid(input_defs=[InputDefinition("left"), InputDefinition("right")])
def subtract(_, left, right):
    return left - right


# start_lazy_repository_definition_marker_0
def load_addition_pipeline():
    @pipeline
    def addition_pipeline():
        return add(return_one(), return_two())

    return addition_pipeline


def load_subtraction_pipeline():
    @pipeline
    def subtraction_pipeline():
        return subtract(return_one(), return_two())

    return subtraction_pipeline


def load_daily_addition_schedule():
    @daily_schedule(
        pipeline_name="addition_pipeline", start_date=datetime.datetime(2020, 1, 1),
    )
    def daily_addition_schedule(date):
        return {}

    return daily_addition_schedule


@repository
def my_lazy_repository():
    # Note that we can pass a dict of functions, rather than a list of
    # pipeline definitions. This allows us to construct pipelines lazily,
    # if, e.g., initializing a pipeline involves any heavy compute
    return {
        "pipelines": {
            "addition_pipeline": load_addition_pipeline,
            "subtraction_pipeline": load_subtraction_pipeline,
        },
        "schedules": {"daily_addition_schedule": load_daily_addition_schedule},
    }


# end_lazy_repository_definition_marker_0
