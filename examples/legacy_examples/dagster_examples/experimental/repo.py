# pylint: disable=no-value-for-parameter


from dagster import (
    pipeline,
    repository,
    solid,
)


@solid
def test_solid(_):
    return 1


@pipeline
def test_pipeline():
    test_solid()

@pipeline
def other_pipeline():
    test_solid()

@repository
def experimental_repository():
    return [test_pipeline]


@repository
def experimental_repository_two():
    return [test_pipeline]
