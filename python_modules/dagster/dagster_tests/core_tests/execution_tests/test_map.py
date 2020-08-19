from dagster import execute_pipeline, lambda_solid, pipeline, solid
from dagster.core.definitions.events import SpecialOutput
from dagster.core.definitions.output import SpecialOutputDefinition


@lambda_solid
def echo(y):
    return y


@solid(output_defs=[SpecialOutputDefinition()])
def emit(_):
    for i in range(3):
        yield SpecialOutput(i, str(i))


@pipeline
def test_pipe():
    # for x in emit():
    # echo(x)
    echo(echo(emit()))


def test_map():

    result = execute_pipeline(test_pipe)
    assert result.success
