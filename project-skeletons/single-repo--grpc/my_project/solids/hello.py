from dagster import SolidExecutionContext, solid


@solid
def hello(_context: SolidExecutionContext) -> str:
    return "Hello, Dagster!"
