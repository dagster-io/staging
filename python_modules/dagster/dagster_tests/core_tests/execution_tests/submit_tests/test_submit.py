import tempfile

import pytest
from dagster import DagsterInvariantViolationError
from dagster.cli.workspace import Workspace
from dagster.core.execution.submit import submit
from dagster.core.test_utils import instance_for_test, instance_for_test_tempdir

from .repo_for_tests import basic_pipeline, dynamic_output_pipeline, pipeline_will_fail


@pytest.mark.parametrize(
    "pipeline, expected_outputs_per_solid",
    [
        (basic_pipeline, [("basic_solid", {"result": 5})]),
        (dynamic_output_pipeline, [("dynamic_numbers", {"result": {"1": 1, "2": 2}})]),
    ],
)
def test_submit_pipeline(pipeline, expected_outputs_per_solid):
    with tempfile.TemporaryDirectory() as tempdir, instance_for_test_tempdir(tempdir) as instance:
        workspace = Workspace.get()
        result = submit(
            workspace,
            pipeline.name,
            instance,
            run_config={"resources": {"io_manager": {"config": {"base_dir": tempdir}}}},
        )
        with result.wait_for_result() as pipeline_result:
            assert result.is_run_complete
            assert pipeline_result.success

            for solid, expected_outputs in expected_outputs_per_solid:
                assert pipeline_result.result_for_node(solid).output_values == expected_outputs


def test_submit_pipeline_does_not_exist():
    with pytest.raises(
        DagsterInvariantViolationError,
        match="Could not find pipeline_doesnt_exist in workspace. Pipelines found: basic_pipeline, "
        "dynamic_output_pipeline, pipeline_will_fail",
    ), instance_for_test() as instance:
        workspace = Workspace.get()
        submit(
            workspace,
            "pipeline_doesnt_exist",
            instance,
            run_config={"resources": {"io_manager": {"config": {"base_dir": "foo"}}}},
        )


def test_submit_pipeline_will_fail():
    with tempfile.TemporaryDirectory() as tempdir, instance_for_test_tempdir(tempdir) as instance:
        workspace = Workspace.get()
        result = submit(
            workspace,
            pipeline_will_fail.name,
            instance,
            run_config={"resources": {"io_manager": {"config": {"base_dir": tempdir}}}},
        )
        with result.wait_for_result() as pipeline_result:
            assert result.is_run_complete
            assert not pipeline_result.success
            assert pipeline_result.result_for_node("basic_solid").output_values["result"] == 5
            failed_result = pipeline_result.result_for_node("solid_will_fail")
            assert not failed_result.success
            with pytest.raises(
                DagsterInvariantViolationError,
                match="Cannot retrieve output values from solid solid_will_fail because execution "
                "of the solid failed.",
            ):
                _ = failed_result.output_values
