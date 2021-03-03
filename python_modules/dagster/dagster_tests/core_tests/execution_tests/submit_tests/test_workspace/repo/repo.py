from dagster import (
    ModeDefinition,
    fs_io_manager,
    multiprocess_executor,
    pipeline,
    repository,
    solid,
)
from dagster.experimental import DynamicOutput, DynamicOutputDefinition


@solid
def basic_solid(_):
    return 5


@solid(output_defs=[DynamicOutputDefinition()])
def dynamic_numbers(_):
    yield DynamicOutput(1, mapping_key="1")
    yield DynamicOutput(2, mapping_key="2")


@pipeline(
    mode_defs=[
        ModeDefinition(
            executor_defs=[multiprocess_executor], resource_defs={"io_manager": fs_io_manager}
        )
    ]
)
def basic_pipeline():
    basic_solid()
    dynamic_numbers()


@repository
def basic_repo():
    return [basic_pipeline]
