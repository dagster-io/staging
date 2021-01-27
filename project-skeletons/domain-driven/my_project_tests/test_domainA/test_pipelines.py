from dagster import execute_pipeline
from my_project.domainA.pipelines import my_pipeline_A


def test_my_pipeline_A():
    result = execute_pipeline(my_pipeline_A, mode="test")

    assert result.success
    assert result.output_for_solid("hello_A") == "Hello, Dagster! (Domain A)"
