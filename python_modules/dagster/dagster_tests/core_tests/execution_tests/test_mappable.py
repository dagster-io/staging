from dagster import (
    DagsterInstance,
    execute_pipeline,
    pipeline,
    reconstructable,
    reexecute_pipeline,
    seven,
    solid,
)
from dagster.core.definitions.events import MappableOutput
from dagster.core.definitions.output import MappableOutputDefinition


@solid
def multiply_by_two(context, y):
    context.log.info("multiply_by_two is returning " + str(y * 2))
    return y * 2


@solid
def multiply_inputs(context, y, ten):
    # current_run = context.instance.get_run_by_id(context.run_id)
    # if y == 2 and current_run.parent_run_id is None:
    #     raise Exception()
    context.log.info("multiply_inputs is returning " + str(y * ten))
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


def test_map():
    result = execute_pipeline(mappable_pipeline,)
    assert result.success


def test_map_multi():
    with seven.TemporaryDirectory() as tmp_dir:
        result = execute_pipeline(
            reconstructable(mappable_pipeline),
            run_config={"storage": {"filesystem": {}}, "execution": {"multiprocess": {}},},
            instance=DagsterInstance.local_temp(tmp_dir),
        )
        assert result.success


def test_reexec_from_parent_basic():
    with seven.TemporaryDirectory() as tmp_dir:
        instance = DagsterInstance.local_temp(tmp_dir)
        parent_result = execute_pipeline(
            mappable_pipeline, run_config={"storage": {"filesystem": {}}}, instance=instance
        )
        parent_run_id = parent_result.run_id

        reexec_result = reexecute_pipeline(
            pipeline=mappable_pipeline,
            parent_run_id=parent_run_id,
            run_config={"storage": {"filesystem": {}},},
            step_selection=["emit.compute"],
            instance=instance,
        )
        assert reexec_result.success


def test_reexec_from_parent_1():
    with seven.TemporaryDirectory() as tmp_dir:
        instance = DagsterInstance.local_temp(tmp_dir)
        parent_result = execute_pipeline(
            mappable_pipeline, run_config={"storage": {"filesystem": {}}}, instance=instance
        )
        parent_run_id = parent_result.run_id

        reexec_result = reexecute_pipeline(
            pipeline=mappable_pipeline,
            parent_run_id=parent_run_id,
            run_config={"storage": {"filesystem": {}},},
            # step_selection=['multiply_inputs[?].compute'], <- not supported, this needs to know all fan outs of previous step, should just run previous step
            step_selection=["multiply_inputs[0].compute"],
            instance=instance,
        )
        assert reexec_result.success


def test_reexec_from_parent_2():
    with seven.TemporaryDirectory() as tmp_dir:
        instance = DagsterInstance.local_temp(tmp_dir)
        parent_result = execute_pipeline(
            mappable_pipeline, run_config={"storage": {"filesystem": {}}}, instance=instance
        )
        parent_run_id = parent_result.run_id

        reexec_result = reexecute_pipeline(
            pipeline=mappable_pipeline,
            parent_run_id=parent_run_id,
            run_config={"storage": {"filesystem": {}},},
            step_selection=["multiply_by_two[0].compute"],
            instance=instance,
        )
        assert reexec_result.success


def test_reexec_from_parent_3():
    with seven.TemporaryDirectory() as tmp_dir:
        instance = DagsterInstance.local_temp(tmp_dir)
        parent_result = execute_pipeline(
            mappable_pipeline, run_config={"storage": {"filesystem": {}}}, instance=instance
        )
        parent_run_id = parent_result.run_id

        reexec_result = reexecute_pipeline(
            pipeline=mappable_pipeline,
            parent_run_id=parent_run_id,
            run_config={"storage": {"filesystem": {}},},
            # step_selection=['multiply_inputs[?].compute'], <- not supported, this needs to know all fan outs of previous step, should just run previous step
            step_selection=["multiply_inputs[0].compute", "multiply_by_two[0].compute"],
            instance=instance,
        )
        assert reexec_result.success
