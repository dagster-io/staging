from dagster import execute_pipeline
from my_project.pipelines import my_pipeline


def test_my_pipeline():
    result = execute_pipeline(my_pipeline, mode="test")

    assert result.success
    assert result.output_for_solid("hello") == "Hello, Dagster!"
