from dagster import graph, op
from dagster.core.execution.api import execute_job


def test_execute_job():
    @op
    def basic():
        return "5"

    @graph
    def basic_graph():
        basic()

    result = execute_job(basic_graph.to_job())
    assert result.success
