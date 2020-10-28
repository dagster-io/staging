from dagster import execute_pipeline, pipeline, solid
from dagster.core.definitions.events import MappableOutput
from dagster.core.definitions.output import MappableOutputDefinition


@solid
def multiply_by_two(context, y):
    context.log.info("echo_again is returning " + str(y * 2))
    return y * 2


@solid
def multiply_inputs(context, y, ten):
    current_run = context.instance.get_run_by_id(context.run_id)
    if current_run.parent_run_id is None:
        raise Exception()
    context.log.info("echo is returning " + str(y * ten))
    return y * ten


@solid
def emit_ten(_):
    return 10


@solid(output_defs=[MappableOutputDefinition()])
def emit(_):
    for i in range(3):
        yield MappableOutput(value=i, mappable_key=str(i))


@pipeline
def mappable_pipeline():
    multiply_by_two(multiply_inputs(emit(), emit_ten()))
