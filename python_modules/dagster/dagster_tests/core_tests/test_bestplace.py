from dagster import Output, pipeline, solid


@solid(version="1")
def take_5(context) -> int:
    context.log.info(f"Take 5")
    return Output(5, address="helloworld")


@pipeline
def bare_minimum():
    take_5()
