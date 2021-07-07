import pytest
from dagster import execute_pipeline, pipeline, solid
from dagster.core.definitions.mode import ModeDefinition
from dagster.core.definitions.reconstructable import reconstructable
from dagster.core.errors import DagsterUnmetExecutorRequirementsError
from dagster.core.storage.fs_io_manager import fs_io_manager
from dagster.core.test_utils import instance_for_test
from dagster_k8s.executor import k8s_job_executor


@solid
def foo():
    return 1


@pipeline(
    mode_defs=[
        ModeDefinition(
            executor_defs=[k8s_job_executor], resource_defs={"io_manager": fs_io_manager}
        )
    ]
)
def bar():
    foo()


def test_requires_k8s_launcher_fail():
    with instance_for_test() as instance:
        with pytest.raises(
            DagsterUnmetExecutorRequirementsError,
            match="This engine is only compatible with a K8sRunLauncher",
        ):
            execute_pipeline(reconstructable(bar), instance=instance)
