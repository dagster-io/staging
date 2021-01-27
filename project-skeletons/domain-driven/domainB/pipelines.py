from dagster import ModeDefinition, pipeline

from .solids import hello_B

MODE_DEV = ModeDefinition(name="dev", resource_defs={})
MODE_TEST = ModeDefinition(name="test", resource_defs={})
MODE_PROD = ModeDefinition(name="prod", resource_defs={})


@pipeline(mode_defs=[MODE_DEV, MODE_TEST, MODE_PROD])
def my_pipeline_B():
    hello_B()
