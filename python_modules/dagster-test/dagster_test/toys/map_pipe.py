from dagster import (
    DagsterInstance,
    execute_pipeline,
    lambda_solid,
    pipeline,
    reconstructable,
    seven,
    solid,
)
from dagster.core.definitions.events import SpecialOutput
from dagster.core.definitions.output import SpecialOutputDefinition


@solid
def echo(context, y):
    if y == 2:
        raise Exception
    return y


@solid
def echo_again(context, y):
    # if y == 2:
    #     raise Exception
    return y


# @lambda_solid
# def echo(y):
#     return y


@solid(output_defs=[SpecialOutputDefinition()])
def emit(_):
    for i in range(3):
        yield SpecialOutput(i, str(i))


@pipeline
def map_pipe():
    # for x in emit():
    # echo(x)
    echo_again(echo(emit()))
