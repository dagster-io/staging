from dagster import execute_pipeline, pipeline, solid


def test_default_config():
    @solid
    def my_solid(context):
        assert context.solid_config == 5

    @pipeline
    def my_pipeline():
        my_solid()

    execute_pipeline(my_pipeline, run_config={"solids": {"my_solid": {"config": 5}}})
