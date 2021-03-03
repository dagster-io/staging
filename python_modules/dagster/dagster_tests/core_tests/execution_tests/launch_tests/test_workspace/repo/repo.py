from dagster import ModeDefinition, multiprocess_executor, pipeline, repository, solid


@solid
def basic_solid(_):
    pass


@pipeline(mode_defs=[ModeDefinition(executor_defs=[multiprocess_executor])])
def basic_pipeline():
    basic_solid()


@repository
def basic_repo():
    return [basic_pipeline]
