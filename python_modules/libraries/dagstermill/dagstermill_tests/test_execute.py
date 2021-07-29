import tempfile

from dagster import ModeDefinition, local_file_manager
from dagstermill.examples.repository import hello_world
from dagstermill.execute import execute_dagstermill_solid


def solid_callable():
    return hello_world


def mode_callable():
    with tempfile.TemporaryDirectory() as tmpdir_path:
        mode_def = ModeDefinition(
            resource_defs={"file_manager": local_file_manager.configured({"base_dir": tmpdir_path})}
        )
    return mode_def


def test_execute():
    # take callable to so it can pass data between processes as code pointer
    res = execute_dagstermill_solid(solid_callable, mode_callable=mode_callable)
    assert res.success
