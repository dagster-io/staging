from dagster import Field, InputDefinition, Nothing, String, pipeline, solid


@solid(version="create_nothing_version", config_schema={"input_str": Field(String)})
def create_nothing(_):
    pass


@solid(
    input_defs=[InputDefinition("nothing_input", Nothing)],
    version="take_nothing_version",
    config_schema={"input_str": Field(String)},
)
def take_nothing(_):
    pass


@solid(
    input_defs=[
        InputDefinition("nothing_input_1", Nothing),
        InputDefinition("nothing_input_2", Nothing),
    ],
    version="take_nothing_two_inputs_version",
    config_schema={"input_str": Field(String)},
)
def take_nothing_two_inputs(_):
    pass


@pipeline
def basic_pipeline():
    take_nothing_two_inputs(
        nothing_input_1=take_nothing.alias("take_nothing_1")(
            create_nothing.alias("create_nothing_1")()
        ),
        nothing_input_2=take_nothing.alias("take_nothing_2")(
            create_nothing.alias("create_nothing_2")()
        ),
    )
