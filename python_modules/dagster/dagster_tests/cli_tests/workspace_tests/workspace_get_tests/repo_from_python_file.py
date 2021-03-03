from dagster import (
    ModeDefinition,
    fs_io_manager,
    multiprocess_executor,
    pipeline,
    repository,
    solid,
)


@solid
def basic_solid(_):
    return 5


@pipeline(
    mode_defs=[
        ModeDefinition(
            executor_defs=[multiprocess_executor], resource_defs={"io_manager": fs_io_manager}
        )
    ]
)
def basic_pipeline_from_file():
    basic_solid()


@repository
def basic_repo_from_file():
    return [basic_pipeline_from_file]
