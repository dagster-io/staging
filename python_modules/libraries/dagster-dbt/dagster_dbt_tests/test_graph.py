from dagster import execute_pipeline, pipeline
from dagster_dbt.graph import graph_from_dbt_project


def test_graph(test_project_dir, dbt_config_dir):
    @pipeline
    def my_pipeline():
        graph_from_dbt_project("dbt_graph", test_project_dir, dbt_config_dir)()

    result = execute_pipeline(my_pipeline)
    assert result.success
