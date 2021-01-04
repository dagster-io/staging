from dagster import execute_pipeline

from ..repo import my_pipeline


def test_pyspark_smoke_test():
    res = execute_pipeline(my_pipeline, mode="smoke_test")
    assert res.success
