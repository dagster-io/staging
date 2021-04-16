from dagster import ModeDefinition, execute_pipeline, make_config_resource, pipeline, solid


@solid(required_resource_keys={"my_config"})
def solid1(context):
    context.log.info("solid1: " + context.resources.my_config)


@solid(required_resource_keys={"my_config"})
def solid2(context):
    context.log.info("solid2: " + context.resources.my_config)


@pipeline(mode_defs=[ModeDefinition(resource_defs={"my_config": make_config_resource()})])
def my_pipeline():
    solid1()
    solid2()


execute_pipeline(my_pipeline, run_config={"resources": {"my_config": {"config": "some_value"}}})
