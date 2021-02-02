from dagster import ModeDefinition, pipeline
from my_project.solids import hello

MODE_DEV = ModeDefinition(name="dev", resource_defs={})
MODE_TEST = ModeDefinition(name="test", resource_defs={})


@pipeline(mode_defs=[MODE_DEV, MODE_TEST])
def my_pipeline():
    hello()
