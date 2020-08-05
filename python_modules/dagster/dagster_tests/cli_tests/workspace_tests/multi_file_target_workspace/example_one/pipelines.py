from solids import example_one_solid

from dagster import pipeline


@pipeline
def example_one_pipeline():
    example_one_solid()
