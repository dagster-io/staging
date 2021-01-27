from dagster import solid


@solid
def add(_context, x: int, y: int) -> int:
    return x + y


@solid
def constant_one(_context) -> int:
    return 1
