from dagster import (  # reexecute_pipeline,
    DagsterInstance,
    execute_pipeline,
    pipeline,
    reconstructable,
    seven,
    solid,
)
from dagster.core.definitions.events import MappableOutput
from dagster.core.definitions.output import MappableOutputDefinition


@solid
def echo_again(context, y):
    context.log.info("echo_again is returning " + str(y * 2))
    return y * 2


@solid
def echo(context, y, ten):
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
def test_pipe():
    echo_again(echo(emit(), emit_ten()))


def test_map():
    result = execute_pipeline(test_pipe)
    assert result.success


def test_map_multi():
    with seven.TemporaryDirectory() as tmp_dir:
        result = execute_pipeline(
            reconstructable(test_pipe),
            run_config={'storage': {'filesystem': {}}, 'execution': {'multiprocess': {}},},
            instance=DagsterInstance.local_temp(tmp_dir),
        )
        assert result.success


# def test_reexec_from_parent():
#     parent_result = execute_pipeline(test_pipe)
#     parent_run_id = parent_result.run_id

#     reexec_result = execute_pipeline(
#         pipeline=test_pipe,
#         run_config={'storage': {'filesystem': {}}},
#         solid_selection=['echo[0].compute'],
#     )


# def test_reexec_from_parent():
#     with seven.TemporaryDirectory() as tmp_dir:
#         instance = DagsterInstance.local_temp(tmp_dir)
#         parent_result = execute_pipeline(
#             test_pipe, run_config={'storage': {'filesystem': {}}}, instance=instance
#         )
#         parent_run_id = parent_result.run_id

#         reexec_result = reexecute_pipeline(
#             pipeline=test_pipe,
#             parent_run_id=parent_run_id,
#             run_config={
#                 'storage': {'filesystem': {}},
#                 "solids": {"echo": {"inputs": {"ten": {"value": 10}}}},
#             },
#             step_keys_to_execute=['echo[0].compute'],
#             instance=instance,
#         )
#         assert reexec_result.success
