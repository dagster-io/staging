from dagster import execute_pipeline
from dagster.cli.workspace import Workspace
from dagster.core.test_utils import instance_for_test


def test_reconstructable():
    with instance_for_test() as instance:
        recon_pipeline = Workspace.reconstructable_from_workspace(
            "dagster_tests/general_tests/workspace_tests/workspace.yaml",
            "basic_pipeline",
        )
        result = execute_pipeline(recon_pipeline, instance=instance)
        assert result.success
        assert result.output_for_solid("basic_solid") == 5
