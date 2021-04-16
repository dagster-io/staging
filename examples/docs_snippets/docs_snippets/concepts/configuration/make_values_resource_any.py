from dagster import ModeDefinition, execute_pipeline, make_values_resource, pipeline, solid


@solid(required_resource_keys={"variable"})
def solid1(context):
    context.log.info(f"variable: {context.resources.variable}")


@solid(required_resource_keys={"variable"})
def solid2(context):
    context.log.info(f"variable: {context.resources.variable}")


@pipeline(mode_defs=[ModeDefinition(resource_defs={"variable": make_values_resource()})])
def my_pipeline():
    solid1()
    solid2()


execute_pipeline(my_pipeline, run_config={"resources": {"variable": {"config": "some_value"}}})
