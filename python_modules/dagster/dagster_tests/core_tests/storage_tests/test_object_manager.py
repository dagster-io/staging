from dagster import ModeDefinition, execute_pipeline, pipeline, solid
from dagster.core.storage.object_manager import ObjectManager, object_manager


def test_object_manager_with_config():
    @solid
    def my_solid(_):
        pass

    class MyObjectManager(ObjectManager):
        def load(self, context):
            assert context.upstream_output.config["some_config"] == "some_value"
            return 1

        def materialize(self, context, obj):
            assert context.config["some_config"] == "some_value"

    @object_manager(output_config_schema={"some_config": str})
    def configurable_object_manager(_):
        return MyObjectManager()

    @pipeline(
        mode_defs=[ModeDefinition(resource_defs={"asset_store": configurable_object_manager})]
    )
    def my_pipeline():
        my_solid()

    run_config = {"solids": {"my_solid": {"outputs": {"result": {"some_config": "some_value"}}}}}
    result = execute_pipeline(my_pipeline, run_config=run_config)
    assert result.output_for_solid("my_solid") == 1
