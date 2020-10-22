from dagster import pipeline, repository, solid


@solid
def do_something(_):
    return 1


@pipeline(name="extra")
def extra_pipeline():
    do_something()


@repository
def extra():
    return {
        "pipelines": {"extra": extra_pipeline},
    }
