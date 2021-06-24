from dagster import graph, op, repository
from dagster.core.definitions.reconstructable import reconstructable_factory
from dagster.core.execution.api import execute_job


@repository
def my_test_repo():
    @op
    def basic():
        return "5"

    @graph
    def basic_graph():
        basic()

    return [basic_graph.to_job()]


def test_execute_job():

    job_pointer = my_test_repo.get_job_pointer("basic_graph")
    result = execute_job(job_pointer=job_pointer)
    assert result.success


def my_repo_factory():
    @repository
    def my_inner_repo():
        @op
        def inner():
            return "5"

        @graph
        def inner_graph():
            inner()

        return [inner_graph.to_job()]

    return my_inner_repo


def test_execute_job_repo_factory():
    job_pointer = reconstructable_factory(my_repo_factory).get_job_pointer("inner_graph")
    result = execute_job(job_pointer)
    assert result.success
