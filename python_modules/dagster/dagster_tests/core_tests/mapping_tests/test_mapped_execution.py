from dagster import execute_pipeline, pipeline, reconstructable, solid
from dagster.core.test_utils import instance_for_test
from dagster.experimental import MappableOutput, MappableOutputDefinition


@solid
def multiply_by_two(context, y):
    context.log.info("multiply_by_two is returning " + str(y * 2))
    return y * 2


@solid
def multiply_inputs(context, y, ten):
    context.log.info("multiply_inputs is returning " + str(y * ten))
    return y * ten


@solid
def emit_ten(_):
    return 10


@solid(output_defs=[MappableOutputDefinition()])
def emit(_):
    for i in range(3):
        yield MappableOutput(value=i, mapping_key=str(i))


@pipeline
def mappable_pipeline():
    multiply_by_two(multiply_inputs(emit(), emit_ten()))


def test_map():
    result = execute_pipeline(mappable_pipeline)
    assert result.success


def test_map_basic():
    with instance_for_test() as instance:
        result = execute_pipeline(reconstructable(mappable_pipeline), instance=instance)
        assert result.success
        keys = result.events_by_step_key.keys()
        assert "multiply_inputs[0]" in keys
        assert "multiply_inputs[1]" in keys
        assert "multiply_inputs[2]" in keys


def test_map_multi():
    with instance_for_test() as instance:
        result = execute_pipeline(
            reconstructable(mappable_pipeline),
            run_config={"storage": {"filesystem": {}}, "execution": {"multiprocess": {}},},
            instance=instance,
        )
        assert result.success
        keys = result.events_by_step_key.keys()
        assert "multiply_inputs[0]" in keys
        assert "multiply_inputs[1]" in keys
        assert "multiply_inputs[2]" in keys
