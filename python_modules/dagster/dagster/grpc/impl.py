'''Workhorse functions for individual API requests.'''

import os
import sys

from dagster import check
from dagster.core.definitions import ScheduleExecutionContext
from dagster.core.definitions.reconstructable import ReconstructablePipeline
from dagster.core.errors import (
    DagsterInvalidSubsetError,
    DagsterSubprocessError,
    PartitionExecutionError,
    ScheduleExecutionError,
    user_code_error_boundary,
)
from dagster.core.events import EngineEventData
from dagster.core.execution.api import execute_run_iterator
from dagster.core.host_representation import external_pipeline_data_from_def
from dagster.core.host_representation.external_data import (
    ExternalPartitionBackfillData,
    ExternalPartitionConfigData,
    ExternalPartitionExecutionErrorData,
    ExternalPartitionNamesData,
    ExternalPartitionTagsData,
    ExternalPipelineSubsetResult,
    ExternalScheduleExecutionData,
    ExternalScheduleExecutionErrorData,
)
from dagster.core.instance import DagsterInstance
from dagster.core.storage.pipeline_run import PipelineRun
from dagster.core.utils import make_new_backfill_id
from dagster.grpc.types import (
    ExternalScheduleExecutionArgs,
    PartitionArgs,
    PartitionNamesArgs,
    ScheduleExecutionDataMode,
)
from dagster.serdes import deserialize_json_to_dagster_namedtuple
from dagster.serdes.ipc import IPCErrorMessage
from dagster.utils import merge_dicts, start_termination_thread
from dagster.utils.error import serializable_error_info_from_exc_info
from dagster.utils.hosted_user_process import recon_repository_from_origin

from .types import ExecuteRunArgs, ExternalScheduleExecutionArgs, PartitionBackfillArgs


class RunInSubprocessComplete:
    '''Sentinel passed over multiprocessing Queue when subprocess is complete'''


class StartRunInSubprocessSuccessful:
    '''Sentinel passed over multiprocessing Queue when launch is successful in subprocess.'''


def _core_execute_run(recon_pipeline, pipeline_run, instance):
    check.inst_param(recon_pipeline, 'recon_pipeline', ReconstructablePipeline)
    check.inst_param(pipeline_run, 'pipeline_run', PipelineRun)
    check.inst_param(instance, 'instance', DagsterInstance)

    try:
        for event in execute_run_iterator(recon_pipeline, pipeline_run, instance):
            yield event
    except DagsterSubprocessError as err:
        if not all(
            [err_info.cls_name == 'KeyboardInterrupt' for err_info in err.subprocess_error_infos]
        ):
            yield instance.report_engine_event(
                'An exception was thrown during execution that is likely a framework error, '
                'rather than an error in user code.',
                pipeline_run,
                EngineEventData.engine_error(serializable_error_info_from_exc_info(sys.exc_info())),
            )
            instance.report_run_failed(pipeline_run)
    except Exception:  # pylint: disable=broad-except
        yield instance.report_engine_event(
            'An exception was thrown during execution that is likely a framework error, '
            'rather than an error in user code.',
            pipeline_run,
            EngineEventData.engine_error(serializable_error_info_from_exc_info(sys.exc_info())),
        )
        instance.report_run_failed(pipeline_run)


def _run_in_subprocess(
    serialized_execute_run_args,
    recon_pipeline,
    termination_event,
    subprocess_status_handler,
    run_event_handler,
):

    start_termination_thread(termination_event)
    try:
        execute_run_args = deserialize_json_to_dagster_namedtuple(serialized_execute_run_args)
        check.inst_param(execute_run_args, 'execute_run_args', ExecuteRunArgs)

        instance = DagsterInstance.from_ref(execute_run_args.instance_ref)
        pipeline_run = instance.get_run_by_id(execute_run_args.pipeline_run_id)

        pid = os.getpid()

    except:  # pylint: disable=bare-except
        event = IPCErrorMessage(
            serializable_error_info=serializable_error_info_from_exc_info(sys.exc_info()),
            message='Error during RPC setup for ExecuteRun',
        )
        subprocess_status_handler(event)
        subprocess_status_handler(RunInSubprocessComplete())
        return

    subprocess_status_handler(StartRunInSubprocessSuccessful())

    run_event_handler(
        instance.report_engine_event(
            'Started process for pipeline (pid: {pid}).'.format(pid=pid),
            pipeline_run,
            EngineEventData.in_process(pid, marker_end='cli_api_subprocess_init'),
        )
    )

    # This is so nasty but seemingly unavoidable
    # https://amir.rachum.com/blog/2017/03/03/generator-cleanup/
    closed = False
    try:
        for event in _core_execute_run(recon_pipeline, pipeline_run, instance):
            run_event_handler(event)
    except KeyboardInterrupt:
        run_event_handler(
            instance.report_engine_event(
                message='Pipeline execution terminated by interrupt', pipeline_run=pipeline_run,
            )
        )
        raise
    except GeneratorExit:
        closed = True
        raise
    finally:
        if not closed:
            run_event_handler(
                instance.report_engine_event(
                    'Process for pipeline exited (pid: {pid}).'.format(pid=pid), pipeline_run,
                )
            )
        subprocess_status_handler(RunInSubprocessComplete())


def execute_run_in_subprocess(
    serialized_execute_run_args, recon_pipeline, event_queue, termination_event
):
    _run_in_subprocess(
        serialized_execute_run_args,
        recon_pipeline,
        termination_event,
        subprocess_status_handler=event_queue.put,
        run_event_handler=event_queue.put,
    )


def start_run_in_subprocess(
    serialized_execute_run_args, recon_pipeline, event_queue, termination_event
):
    _run_in_subprocess(
        serialized_execute_run_args,
        recon_pipeline,
        termination_event,
        subprocess_status_handler=event_queue.put,
        run_event_handler=lambda x: None,
    )


def get_external_pipeline_subset_result(recon_pipeline, solid_selection):
    check.inst_param(recon_pipeline, 'recon_pipeline', ReconstructablePipeline)

    if solid_selection:
        try:
            sub_pipeline = recon_pipeline.subset_for_execution(solid_selection)
            definition = sub_pipeline.get_definition()
        except DagsterInvalidSubsetError:
            return ExternalPipelineSubsetResult(
                success=False, error=serializable_error_info_from_exc_info(sys.exc_info())
            )
    else:
        definition = recon_pipeline.get_definition()

    external_pipeline_data = external_pipeline_data_from_def(definition)
    return ExternalPipelineSubsetResult(success=True, external_pipeline_data=external_pipeline_data)


def get_external_schedule_execution(external_schedule_execution_args):
    check.inst_param(
        external_schedule_execution_args,
        'external_schedule_execution_args',
        ExternalScheduleExecutionArgs,
    )

    recon_repo = recon_repository_from_origin(external_schedule_execution_args.repository_origin)
    definition = recon_repo.get_definition()
    schedule_def = definition.get_schedule_def(external_schedule_execution_args.schedule_name)
    instance = DagsterInstance.from_ref(external_schedule_execution_args.instance_ref)
    schedule_context = ScheduleExecutionContext(instance)
    schedule_execution_data_mode = external_schedule_execution_args.schedule_execution_data_mode

    try:
        with user_code_error_boundary(
            ScheduleExecutionError,
            lambda: 'Error occurred during the execution of should_execute for schedule '
            '{schedule_name}'.format(schedule_name=schedule_def.name),
        ):
            should_execute = None
            if schedule_execution_data_mode == ScheduleExecutionDataMode.LAUNCH_SCHEDULED_EXECUTION:
                should_execute = schedule_def.should_execute(schedule_context)
                if not should_execute:
                    return ExternalScheduleExecutionData(
                        should_execute=False, run_config=None, tags=None
                    )

        with user_code_error_boundary(
            ScheduleExecutionError,
            lambda: 'Error occurred during the execution of run_config_fn for schedule '
            '{schedule_name}'.format(schedule_name=schedule_def.name),
        ):
            run_config = schedule_def.get_run_config(schedule_context)

        with user_code_error_boundary(
            ScheduleExecutionError,
            lambda: 'Error occurred during the execution of tags_fn for schedule '
            '{schedule_name}'.format(schedule_name=schedule_def.name),
        ):
            tags = schedule_def.get_tags(schedule_context)

        return ExternalScheduleExecutionData(
            run_config=run_config, tags=tags, should_execute=should_execute
        )
    except ScheduleExecutionError:
        return ExternalScheduleExecutionErrorData(
            serializable_error_info_from_exc_info(sys.exc_info())
        )


def get_partition_config(args):
    check.inst_param(args, 'args', PartitionArgs)
    recon_repo = recon_repository_from_origin(args.repository_origin)
    definition = recon_repo.get_definition()
    partition_set_def = definition.get_partition_set_def(args.partition_set_name)
    partition = partition_set_def.get_partition(args.partition_name)
    try:
        with user_code_error_boundary(
            PartitionExecutionError,
            lambda: 'Error occurred during the evaluation of the `run_config_for_partition` '
            'function for partition set {partition_set_name}'.format(
                partition_set_name=partition_set_def.name
            ),
        ):
            run_config = partition_set_def.run_config_for_partition(partition)
            return ExternalPartitionConfigData(name=partition.name, run_config=run_config)
    except PartitionExecutionError:
        return ExternalPartitionExecutionErrorData(
            serializable_error_info_from_exc_info(sys.exc_info())
        )


def get_partition_names(args):
    check.inst_param(args, 'args', PartitionNamesArgs)
    recon_repo = recon_repository_from_origin(args.repository_origin)
    definition = recon_repo.get_definition()
    partition_set_def = definition.get_partition_set_def(args.partition_set_name)
    try:
        with user_code_error_boundary(
            PartitionExecutionError,
            lambda: 'Error occurred during the execution of the partition generation function for '
            'partition set {partition_set_name}'.format(partition_set_name=partition_set_def.name),
        ):
            return ExternalPartitionNamesData(
                partition_names=partition_set_def.get_partition_names()
            )
    except PartitionExecutionError:
        return ExternalPartitionExecutionErrorData(
            serializable_error_info_from_exc_info(sys.exc_info())
        )


def get_partition_tags(args):
    check.inst_param(args, 'args', PartitionArgs)
    recon_repo = recon_repository_from_origin(args.repository_origin)
    definition = recon_repo.get_definition()
    partition_set_def = definition.get_partition_set_def(args.partition_set_name)
    partition = partition_set_def.get_partition(args.partition_name)
    try:
        with user_code_error_boundary(
            PartitionExecutionError,
            lambda: 'Error occurred during the evaluation of the `tags_for_partition` function for '
            'partition set {partition_set_name}'.format(partition_set_name=partition_set_def.name),
        ):
            tags = partition_set_def.tags_for_partition(partition)
            return ExternalPartitionTagsData(name=partition.name, tags=tags)
    except PartitionExecutionError:
        return ExternalPartitionExecutionErrorData(
            serializable_error_info_from_exc_info(sys.exc_info())
        )


def launch_partition_backfill(args):
    check.inst_param(args, 'args', PartitionBackfillArgs)
    instance = DagsterInstance.from_ref(args.instance_ref)
    recon_repo = recon_repository_from_origin(args.repository_origin)
    repo_definition = recon_repo.get_definition()
    partition_set_def = repo_definition.get_partition_set_def(args.partition_set_name)
    pipeline_def = repo_definition.get_pipeline(partition_set_def.pipeline_name)

    repo_location = _build_location_for_reconstructable_repo(recon_repo)

    external_pipeline = repo_location.get_repository(
        recon_repo.get_definition().name
    ).get_full_external_pipeline(pipeline_def.name)
    try:
        with user_code_error_boundary(
            PartitionExecutionError,
            lambda: 'Error occurred during the partition generation for partition set '
            '{partition_set_name}'.format(partition_set_name=partition_set_def.name),
        ):
            all_partitions = partition_set_def.get_partitions()
        partitions = [
            partition for partition in all_partitions if partition.name in args.partition_names
        ]
        backfill_id = make_new_backfill_id()
        run_tags = merge_dicts(PipelineRun.tags_for_backfill_id(backfill_id), args.tags)
        for partition in partitions:

            def _error_message_fn(partition_set_name, partition_name):
                return lambda: (
                    'Error occurred during the partition config and tag generation for '
                    'partition set {partition_set_name}::{partition_name}'.format(
                        partition_set_name=partition_set_name, partition_name=partition_name
                    )
                )

            with user_code_error_boundary(
                PartitionExecutionError, _error_message_fn(partition_set_def.name, partition.name)
            ):
                run_config = partition_set_def.run_config_for_partition(partition)
                tags = merge_dicts(partition_set_def.tags_for_partition(partition), run_tags)

            run = instance.create_run_for_pipeline(
                pipeline_def=pipeline_def,
                mode=partition_set_def.mode,
                solids_to_execute=frozenset(partition_set_def.solid_selection)
                if partition_set_def.solid_selection
                else None,
                run_config=run_config,
                tags=tags,
            )

            instance.launch_run(run.run_id, external_pipeline)
        return ExternalPartitionBackfillData(backfill_id=backfill_id)

    except PartitionExecutionError:
        return ExternalPartitionExecutionErrorData(
            serializable_error_info_from_exc_info(sys.exc_info())
        )


def _build_location_for_reconstructable_repo(recon_repo):
    # construct in process repository location to execute the repo

    from dagster.core.host_representation.repository_location import InProcessRepositoryLocation

    return InProcessRepositoryLocation(recon_repo)
