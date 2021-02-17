from os import path

import pytest
from dagster import (
    ModeDefinition,
    PipelineDefinition,
    check,
    execute_pipeline,
    fs_io_manager,
    multiprocess_executor,
    pipeline,
    reconstructable,
    solid,
)
from dagster.core.definitions.executor import executor
from dagster.core.execution.retries import Retries
from dagster.core.test_utils import instance_for_test


def assert_pipeline_runs_with_executor(executor_defs, execution_config):
    it = {}

    @solid
    def a_solid(_):
        it["ran"] = True

    pipeline_def = PipelineDefinition(
        name="testing_pipeline",
        solid_defs=[a_solid],
        mode_defs=[ModeDefinition(executor_defs=executor_defs)],
    )

    result = execute_pipeline(pipeline_def, {"execution": execution_config})
    assert result.success
    assert it["ran"]


@pytest.mark.xfail(raises=check.ParameterCheckError)
def test_in_process_executor_primitive_config():
    @executor(name="test_executor", config_schema=str)
    def test_executor(init_context):
        from dagster.core.executor.in_process import InProcessExecutor

        assert init_context.executor_config == "secret testing value!!"

        return InProcessExecutor(
            # shouldn't need to .get() here - issue with defaults in config setup
            retries=Retries.from_config({"enabled": {}}),
            marker_to_close=None,
        )

    assert_pipeline_runs_with_executor(
        [test_executor], {"test_executor": {"config": "secret testing value!!"}}
    )


def test_in_process_executor_dict_config():
    @executor(name="test_executor", config_schema={"value": str})
    def test_executor(init_context):
        from dagster.core.executor.in_process import InProcessExecutor

        assert init_context.executor_config["value"] == "secret testing value!!"

        return InProcessExecutor(
            # shouldn't need to .get() here - issue with defaults in config setup
            retries=Retries.from_config({"enabled": {}}),
            marker_to_close=None,
        )

    assert_pipeline_runs_with_executor(
        [test_executor], {"test_executor": {"config": {"value": "secret testing value!!"}}}
    )


def test_in_process_executor_dict_config_configured():
    @executor(name="test_executor", config_schema={"value": str})
    def test_executor(init_context):
        from dagster.core.executor.in_process import InProcessExecutor

        assert init_context.executor_config["value"] == "secret testing value!!"

        return InProcessExecutor(
            # shouldn't need to .get() here - issue with defaults in config setup
            retries=Retries.from_config({"enabled": {}}),
            marker_to_close=None,
        )

    test_executor_configured = test_executor.configured(
        {"value": "secret testing value!!"}, "configured_test_executor"
    )

    assert_pipeline_runs_with_executor(
        [test_executor_configured], {"configured_test_executor": None}
    )


@solid
def emit_one(_):
    return 1


@pipeline(
    mode_defs=[
        ModeDefinition(
            executor_defs=[multiprocess_executor.configured({"max_concurrent": 1})],
            resource_defs={"io_manager": fs_io_manager},
        )
    ]
)
def multiproc_test():
    emit_one()


def test_multiproc():

    with instance_for_test() as instance:

        result = execute_pipeline(
            reconstructable(multiproc_test),
            # https://github.com/dagster-io/dagster/issues/3697
            # remove the execution part of this config
            run_config={
                "execution": {"multiprocess": {}},
                "resources": {
                    "io_manager": {
                        "config": {"base_dir": path.join(instance.root_directory, "storage")}
                    }
                },
            },
            instance=instance,
        )
        assert result.success
