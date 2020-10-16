from dagster import pipeline, solid
from dagster.core.definitions.events import SpecialOutput
from dagster.core.definitions.output import SpecialOutputDefinition


@solid
def echo(context, y):
    # this one works w/ re-execution
    # if y == 2:
    #     run = context.instance.get_run_by_id(context.run_id)
    #     if run.parent_run_id is None:
    #         raise Exception()
    return y


@solid
def echo_again(context, y):
    # this one does not work w/ re-execution
    if y == 2:
        run = context.instance.get_run_by_id(context.run_id)
        if run.parent_run_id is None:
            raise Exception()
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
