from dagster import execute_pipeline
from docs_sipppets.concepts.io_managers.metadata import my_pipeline


def test_metadata():
    execute_pipeline(my_pipeline)
