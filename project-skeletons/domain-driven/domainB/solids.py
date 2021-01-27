from dagster import SolidExecutionContext, solid


@solid
def hello_B(_context: SolidExecutionContext) -> str:
    return "Hello, Dagster! (Domain B)"
