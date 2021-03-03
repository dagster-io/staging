from dagster import (
    InputDefinition,
    ModeDefinition,
    fs_io_manager,
    multiprocess_executor,
    pipeline,
    repository,
    solid,
)
from dagster.experimental import DynamicOutput, DynamicOutputDefinition


def get_fs_mode():
    return ModeDefinition(
        resource_defs={"io_manager": fs_io_manager}, executor_defs=[multiprocess_executor]
    )


@solid
def basic_solid(_):
    return 5


@pipeline(mode_defs=[get_fs_mode()])
def basic_pipeline():
    basic_solid()


@solid(output_defs=[DynamicOutputDefinition()])
def dynamic_numbers(_):
    yield DynamicOutput(1, mapping_key="1")
    yield DynamicOutput(2, mapping_key="2")


@pipeline(mode_defs=[get_fs_mode()])
def dynamic_output_pipeline():
    dynamic_numbers()


@solid(input_defs=[InputDefinition("_inp")])
def solid_will_fail(_, _inp):
    raise Exception


@pipeline(mode_defs=[get_fs_mode()])
def pipeline_will_fail():
    solid_will_fail(basic_solid())


@repository
def testing_repo():
    return [basic_pipeline, dynamic_output_pipeline, pipeline_will_fail]
