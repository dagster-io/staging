from dagster import pipeline

from .baz import baz_solid


@pipeline
def bar_pipeline():
    baz_solid()
