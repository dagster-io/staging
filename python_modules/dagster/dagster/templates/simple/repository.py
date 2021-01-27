from dagster import repository

from .pipelines import add_ones


@repository
def my_repository():
    return [add_ones]
