from dagster import Bool, Int, List, PipelineDefinition, lambda_solid
from dagster.core.definitions.events import ObjectStoreOperationType
from dagster.core.execution.api import create_execution_plan, execute_plan
from dagster.core.execution.plan.objects import StepOutputHandle
from dagster.core.instance import DagsterInstance
from dagster.core.storage.intermediates_manager import build_fs_intermediate_storage


def define_intermediates_manager_from_pipeline(pipeline_to_run, solid_to_run):
    pipeline = pipeline_to_run()

    run_config = {'intermediate_storage': {'filesystem': {}}}

    instance = DagsterInstance.ephemeral()
    execution_plan = create_execution_plan(pipeline, run_config=run_config,)
    pipeline_run = instance.create_run_for_pipeline(
        pipeline_def=pipeline, execution_plan=execution_plan
    )
    solid_compute = '{solid_name}.compute'.format(solid_name=solid_to_run)
    assert execution_plan.get_step_by_key(solid_compute)

    list(execute_plan(execution_plan, instance, run_config=run_config, pipeline_run=pipeline_run,))

    return build_fs_intermediate_storage(instance.intermediates_directory, pipeline_run.run_id)


def define_singletype_pipeline():
    @lambda_solid
    def return_one():
        return 1

    pipeline = PipelineDefinition(name='basic_external_plan_execution', solid_defs=[return_one])
    return pipeline


def test_file_system_intermediate_storage():
    intermediates_manager = define_intermediates_manager_from_pipeline(
        define_singletype_pipeline, 'return_one'
    )

    assert intermediates_manager.has_intermediate(None, StepOutputHandle('return_one.compute'))
    assert (
        intermediates_manager.get_intermediate(
            None, Int, StepOutputHandle('return_one.compute')
        ).obj
        == 1
    )
    assert (
        intermediates_manager.rm_intermediate(None, StepOutputHandle('return_one.compute')).op
        == ObjectStoreOperationType.RM_OBJECT
    )

    assert (
        intermediates_manager.set_intermediate(
            None, Int, StepOutputHandle('return_one.compute'), 42
        ).op
        == ObjectStoreOperationType.SET_OBJECT
    )

    assert (
        intermediates_manager.get_intermediate(
            None, Int, StepOutputHandle('return_one.compute')
        ).obj
        == 42
    )


def define_composite_type_pipeline():
    @lambda_solid
    def return_true_lst():
        return [True]

    pipeline = PipelineDefinition(
        name='basic_external_plan_execution', solid_defs=[return_true_lst]
    )
    return pipeline


def test_file_system_intermediate_storage_composite_types():
    intermediates_manager = define_intermediates_manager_from_pipeline(
        define_composite_type_pipeline, 'return_true_lst'
    )

    assert intermediates_manager.has_intermediate(None, StepOutputHandle('return_true_lst.compute'))

    assert intermediates_manager.get_intermediate(
        None, List[Bool], StepOutputHandle('return_true_lst.compute')
    ).obj == [True]
