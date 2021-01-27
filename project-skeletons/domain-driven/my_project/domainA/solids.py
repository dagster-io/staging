from dagster import SolidExecutionContext, solid


@solid
def hello_A(_context: SolidExecutionContext) -> str:
    return "Hello, Dagster! (Domain A)"
