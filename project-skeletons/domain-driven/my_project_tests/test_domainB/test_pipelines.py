from dagster import execute_pipeline
from my_project.domainB.pipelines import my_pipeline_B


def test_my_pipeline_B():
    result = execute_pipeline(my_pipeline_B, mode="test")

    assert result.success
    assert result.output_for_solid("hello_B") == "Hello, Dagster! (Domain B)"
