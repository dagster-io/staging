from dagster import ModeDefinition, execute_pipeline, make_variables_resource, pipeline, solid


@solid(required_resource_keys={"variables"})
def solid1(context):
    context.log.info(f"my str: {context.resources.variables['my_str']}")


@solid(required_resource_keys={"variables"})
def solid2(context):
    context.log.info(f"my int: {context.resources.variables['my_int']}")


@pipeline(
    mode_defs=[
        ModeDefinition(resource_defs={"variables": make_variables_resource(my_str=str, my_int=int)})
    ]
)
def my_pipeline():
    solid1()
    solid2()


execute_pipeline(
    my_pipeline, run_config={"resources": {"variables": {"config": {"my_str": "foo", "my_int": 1}}}}
)
