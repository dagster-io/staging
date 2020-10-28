from dagster import (  # DagsterInstance,; lambda_solid,; reconstructable,; seven,
    execute_pipeline,
    pipeline,
    solid,
)
from dagster.core.definitions.events import MappableOutput
from dagster.core.definitions.output import MappableOutputDefinition


@solid
def echo_again(context, y):
    context.log.info('echo_again is returning ' + str(y * 2))
    return y * 2


@solid
def echo(context, y):
    context.log.info('echo is returning ' + str(y))
    return y


@solid(output_defs=[MappableOutputDefinition()])
# @solid
def emit(_):
    for i in range(3):
        yield MappableOutput(value=i, mappable_key=str(i))


@pipeline
def test_pipe():
    echo_again(echo(emit()))


def test_map():
    result = execute_pipeline(test_pipe)
    assert result.success


# def test_map_multi():
#     with seven.TemporaryDirectory() as tmp_dir:
#         result = execute_pipeline(
#             reconstructable(test_pipe),
#             run_config={'storage': {'filesystem': {}}, 'execution': {'multiprocess': {}},},
#             instance=DagsterInstance.local_temp(tmp_dir),
#         )
#         assert result.success
