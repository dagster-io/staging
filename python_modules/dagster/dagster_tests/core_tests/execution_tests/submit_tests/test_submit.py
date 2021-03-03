import tempfile

from dagster.core.execution.submit import submit
from dagster.core.test_utils import instance_for_test_tempdir

from .test_workspace.repo.repo import basic_pipeline


def test_submit_from_pipeline_def():
    with tempfile.TemporaryDirectory() as tempdir, instance_for_test_tempdir(tempdir) as instance:
        result = submit(
            basic_pipeline,
            instance,
            run_config={"resources": {"io_manager": {"config": {"base_dir": tempdir}}}},
        )
        with result.wait_for_result() as pipeline_result:
            assert pipeline_result.result_for_node("basic_solid").output_values["result"] == 5
            dynamic_output_vals = pipeline_result.result_for_node("dynamic_numbers").output_values[
                "result"
            ]
            assert dynamic_output_vals["1"] == 1
            assert dynamic_output_vals["2"] == 2
