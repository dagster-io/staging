import sys

import pendulum

from dagster import check
from dagster.core.errors import DagsterSubprocessError
from dagster.core.events import EngineEventData
from dagster.core.host_representation import (
    ExternalPipeline,
    PipelineSelector,
    RepositoryLocation,
    RepositoryLocationHandle,
)
from dagster.core.host_representation.external_data import (
    ExternalSensorExecutionData,
    ExternalSensorExecutionErrorData,
)
from dagster.core.instance import DagsterInstance
from dagster.core.storage.pipeline_run import PipelineRunStatus
from dagster.utils.error import serializable_error_info_from_exc_info
from dagster.core.definitions.job import JobType
from dagster.core.scheduler.job import JobStatus, JobTickStatus, JobTickData

RESOLVED_TICK_STATES = [JobTickStatus.SUCCESS, JobTickStatus.FAILURE]


class JobTickContext:
    def __init__(self, tick, instance, logger):
        self._tick = tick
        self._instance = instance
        self._logger = logger

    @property
    def status(self):
        return self._tick.status

    @property
    def logger(self):
        return self._logger

    def update_with_status(self, status, **kwargs):
        self._tick = self._tick.with_status(status=status, **kwargs)

    def _write(self):
        self._instance.update_job_tick(self._tick)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if exception_value and not isinstance(exception_value, KeyboardInterrupt):
            error_data = serializable_error_info_from_exc_info(sys.exc_info())
            self.update_with_status(JobTickStatus.FAILURE, error=error_data)
            self._write()
            self._logger.error(
                "Error launching scheduled run: {error_info}".format(
                    error_info=error_data.to_string()
                ),
            )
            return True  # Swallow the exception after logging in the tick DB

        self._write()


def launch_sensor_runs(instance, logger):
    check.inst_param(instance, "instance", DagsterInstance)
    sensor_jobs = [
        s
        for s in instance.all_stored_job_state(job_type=JobType.SENSOR)
        if s.status == JobStatus.RUNNING
    ]
    if not sensor_jobs:
        logger.info("Not checking for any runs since no sensors have been started.")
        return
    logger.info(
        "Checking for new runs for the following sensors: {sensor_names}".format(
            sensor_names=", ".join([job.job_name for job in sensor_jobs]),
        )
    )

    for job_state in sensor_jobs:
        try:
            with RepositoryLocationHandle.create_from_repository_origin(
                job_state.origin.repository_origin, instance
            ) as repo_location_handle:
                now = pendulum.now()
                latest_tick = instance.get_latest_job_tick(job_state.job_origin_id)
                if not latest_tick or latest_tick.status in RESOLVED_TICK_STATES:
                    tick = instance.create_job_tick(
                        JobTickData(
                            job_origin_id=job_state.job_origin_id,
                            job_name=job_state.job_name,
                            job_type=JobType.SENSOR,
                            status=JobTickStatus.STARTED,
                            timestamp=now.timestamp(),
                        )
                    )
                else:
                    tick = latest_tick.with_status(JobTickStatus.STARTED, timestamp=now.timestamp())
                    instance.update_job_tick(tick)

                last_checked_time = latest_tick.timestamp if latest_tick else None

                repo_location = RepositoryLocation.from_handle(repo_location_handle)
                repo_dict = repo_location.get_repositories()
                check.invariant(
                    len(repo_dict) == 1,
                    "Reconstructed repository location should have exactly one repository",
                )
                external_repo = next(iter(repo_dict.values()))
                external_sensor = external_repo.get_external_sensor(job_state.job_name)
                with JobTickContext(tick, instance, logger) as tick_context:
                    launch_runs_for_sensor(
                        tick_context,
                        instance,
                        repo_location,
                        external_repo,
                        external_sensor,
                        last_checked_time,
                    )
        except Exception:  # pylint: disable=broad-except
            logger.error(
                "Sensor failed for {sensor_name} : {error_info}".format(
                    sensor_name=job_state.job_name,
                    error_info=serializable_error_info_from_exc_info(sys.exc_info()).to_string(),
                )
            )


def launch_runs_for_sensor(
    context, instance, repo_location, external_repo, external_sensor, last_checked_time
):
    sensor_runtime_data = repo_location.get_external_sensor_execution_data(
        instance, external_repo.handle, external_sensor.name, last_checked_time
    )
    if isinstance(sensor_runtime_data, ExternalSensorExecutionErrorData):
        context.logger.error(
            "Sensor failed for {sensor_name} : {error_info}".format(
                sensor_name=external_sensor.name, error_info=sensor_runtime_data.error
            )
        )
        context.update_with_status(JobTickStatus.FAILURE, error=sensor_runtime_data.error)
        return

    assert isinstance(sensor_runtime_data, ExternalSensorExecutionData)
    if not sensor_runtime_data.job_params_list:
        context.logger.info(
            "Sensor returned false for {sensor_name}, skipping".format(
                sensor_name=external_sensor.name
            )
        )
        context.update_with_status(JobTickStatus.SKIPPED)
        return

    pipeline_selector = PipelineSelector(
        location_name=repo_location.name,
        repository_name=external_repo.name,
        pipeline_name=external_sensor.pipeline_name,
        solid_selection=external_sensor.solid_selection,
    )
    subset_pipeline_result = repo_location.get_subset_external_pipeline_result(pipeline_selector)
    external_pipeline = ExternalPipeline(
        subset_pipeline_result.external_pipeline_data, external_repo.handle,
    )

    for job_params in sensor_runtime_data.job_params_list:
        execution_plan_errors = []
        execution_plan_snapshot = None
        try:
            external_execution_plan = repo_location.get_external_execution_plan(
                external_pipeline,
                job_params.run_config,
                external_sensor.mode,
                step_keys_to_execute=None,
            )
            execution_plan_snapshot = external_execution_plan.execution_plan_snapshot
        except DagsterSubprocessError as e:
            execution_plan_errors.extend(e.subprocess_error_infos)
        except Exception as e:  # pylint: disable=broad-except
            execution_plan_errors.append(serializable_error_info_from_exc_info(sys.exc_info()))

        run = instance.create_run(
            pipeline_name=external_sensor.pipeline_name,
            run_id=None,
            run_config=job_params.run_config,
            mode=external_sensor.mode,
            solids_to_execute=external_pipeline.solids_to_execute,
            step_keys_to_execute=None,
            solid_selection=external_sensor.solid_selection,
            status=(
                PipelineRunStatus.FAILURE
                if len(execution_plan_errors) > 0
                else PipelineRunStatus.NOT_STARTED
            ),
            root_run_id=None,
            parent_run_id=None,
            tags=job_params.tags,
            pipeline_snapshot=external_pipeline.pipeline_snapshot,
            execution_plan_snapshot=execution_plan_snapshot,
            parent_pipeline_snapshot=external_pipeline.parent_pipeline_snapshot,
        )

        if len(execution_plan_errors) > 0:
            for error in execution_plan_errors:
                instance.report_engine_event(
                    error.message, run, EngineEventData.engine_error(error),
                )
            instance.report_run_failed(run)
            context.logger.error(
                "Failed to fetch execution plan for {sensor_name}: {error_string}".format(
                    sensor_name=external_sensor.name,
                    error_string="\n".join([error.to_string() for error in execution_plan_errors]),
                ),
            )
            continue

        try:
            instance.launch_run(run.run_id, external_pipeline)
            context.logger.info(
                "Completed launch of run {run_id} for {sensor_name}".format(
                    run_id=run.run_id, sensor_name=external_sensor.name
                )
            )
            context.update_with_status(JobTickStatus.SUCCESS, run_id=run.run_id)
        except Exception as e:  # pylint: disable=broad-except
            if not isinstance(e, KeyboardInterrupt):
                error = serializable_error_info_from_exc_info(sys.exc_info())
                instance.report_engine_event(
                    error.message, run, EngineEventData.engine_error(error),
                )
                instance.report_run_failed(run)
                context.logger.error(
                    "Run {run_id} created successfully but failed to launch.".format(
                        run_id=run.run_id
                    )
                )
