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


# def test_map():
#     result = execute_pipeline(test_pipe)
#     assert result.success


def test_map_multi():
    with seven.TemporaryDirectory() as tmp_dir:
        result = execute_pipeline(
            reconstructable(test_pipe),
            run_config={'storage': {'filesystem': {}}, 'execution': {'multiprocess': {}},},
            instance=DagsterInstance.local_temp(tmp_dir),
        )
        assert result.success
