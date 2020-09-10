from dagster import DagsterInstance, Int, pipeline, solid
from dagster.core.definitions import InputDefinition
from dagster.core.storage.tags import MEMOIZED_RUN_TAG


@solid(version="42")
def versioned_solid_no_input(_):
    return 4


@solid(version="5")
def versioned_solid_takes_input(_, intput):
    return 2 * intput


@pipeline
def versioned_pipeline():
    return versioned_solid_takes_input(versioned_solid_no_input())


def test_versioned_execution_plan_no_external_dependencies():
    instance = DagsterInstance.ephemeral()
    pipeline_run = instance.create_run_for_pipeline(
        pipeline_def=versioned_pipeline, tags={MEMOIZED_RUN_TAG: "true"}
    )
    assert "versioned_solid_no_input.compute" in pipeline_run.step_keys_to_execute
    assert "versioned_solid_takes_input.compute" in pipeline_run.step_keys_to_execute
    assert len(pipeline_run.step_keys_to_execute) == 2


@solid(version="42", input_defs=[InputDefinition("intpt", Int)])
def versioned_solid_ext_input(_, intpt):
    return intpt * 4


@pipeline
def versioned_pipeline_ext_input():
    return versioned_solid_takes_input(versioned_solid_ext_input())


def test_external_dependencies():
    instance = DagsterInstance.ephemeral()
    pipeline_run = instance.create_run_for_pipeline(
        pipeline_def=versioned_pipeline_ext_input,
        tags={MEMOIZED_RUN_TAG: "true"},
        run_config={"solids": {"versioned_solid_ext_input": {"inputs": {"intpt": 3}}}},
    )
    assert "versioned_solid_ext_input.compute" in pipeline_run.step_keys_to_execute
    assert "versioned_solid_takes_input.compute" in pipeline_run.step_keys_to_execute
    assert len(pipeline_run.step_keys_to_execute) == 2


@solid
def basic_solid(_):
    return 5


@solid
def basic_takes_input_solid(_, intpt):
    return intpt * 4


@pipeline
def default_version_pipeline():
    return basic_takes_input_solid(basic_solid())


def test_default_version():
    instance = DagsterInstance.ephemeral()
    pipeline_run = instance.create_run_for_pipeline(
        pipeline_def=default_version_pipeline, tags={MEMOIZED_RUN_TAG: "true"},
    )

    assert "basic_solid.compute" in pipeline_run.step_keys_to_execute
    assert "basic_takes_input_solid.compute" in pipeline_run.step_keys_to_execute
    assert len(pipeline_run.step_keys_to_execute) == 2
