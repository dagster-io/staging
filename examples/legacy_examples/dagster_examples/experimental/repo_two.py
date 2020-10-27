# pylint: disable=no-value-for-parameter

from dagster import pipeline, repository, solid


@pipeline
def test_pipeline():
    @solid
    def test_solid(_):
        return 1

    test_solid()


@repository
def experimental_repository():
    return [test_pipeline]
