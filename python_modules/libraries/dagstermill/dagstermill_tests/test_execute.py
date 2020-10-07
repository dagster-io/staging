from dagstermill.examples.repository import define_hello_world_with_output
from dagstermill.execute import execute_dagstermill_solid


def test_execute_dagstermill_solid():
    res = execute_dagstermill_solid(define_hello_world_with_output)
    assert res.success
