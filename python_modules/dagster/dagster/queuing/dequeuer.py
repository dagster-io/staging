from dagster import check
from dagster.core.host_representation import (
    ExternalPipeline,
    PipelineSelector,
    RepositoryLocation,
    RepositoryLocationHandle,
)
from dagster.core.instance import DagsterInstance
from dagster.core.origin import PipelineOrigin
from dagster.core.storage.pipeline_run import PipelineRun, PipelineRunStatus, PipelineRunsFilter

IN_PROGRESS_STATUSES = [
    PipelineRunStatus.NOT_STARTED,
    PipelineRunStatus.STARTED,
    # PipelineRunStatus.MANAGED, # Note: currently not available on Managed runs
]


class DagsterDequeuer:
    def __init__(self, instance, max_concurrent_runs=10):
        self._instance = check.inst_param(instance, "instance", DagsterInstance)
        self._max_concurrent_runs = max_concurrent_runs

    def _get_queued_runs(self, limit=None):
        queued_runs_filter = PipelineRunsFilter(status=PipelineRunStatus.QUEUED)

        # TODO: filter to only run_id column
        runs = self._instance.get_runs(filters=queued_runs_filter, limit=limit)
        assert len(runs) <= limit
        return runs

    def _count_in_progress_runs(self):
        num_runs = 0

        # TODO: combine to single query
        for status in IN_PROGRESS_STATUSES:
            runs_filter = PipelineRunsFilter(status=status)
            num_runs += self._instance.get_runs_count(filters=runs_filter)

        return num_runs

    def _get_external_pipeline(self, pipeline_run):
        check.inst_param(pipeline_run, "pipeline_run", PipelineRun)
        pipeline_origin = check.inst(pipeline_run.pipeline_origin, PipelineOrigin)

        with RepositoryLocationHandle.create_from_repository_origin(
            pipeline_origin.repository_origin, self._instance
        ) as repo_location_handle:
            repo_location = RepositoryLocation.from_handle(repo_location_handle)

            repo_dict = repo_location.get_repositories()
            check.invariant(
                len(repo_dict) == 1,
                "Reconstructed repository location should have exactly one repository",
            )
            external_repo = next(iter(repo_dict.values()))

            pipeline_selector = PipelineSelector(
                location_name=repo_location.name,
                repository_name=external_repo.name,
                pipeline_name=pipeline_run.pipeline_name,
                solid_selection=pipeline_run.solid_selection,
            )

            subset_pipeline_result = repo_location.get_subset_external_pipeline_result(
                pipeline_selector
            )
            external_pipeline = ExternalPipeline(
                subset_pipeline_result.external_pipeline_data, external_repo.handle,
            )
            return external_pipeline

    def attempt_to_launch_runs(self):
        max_runs_to_launch = self._max_concurrent_runs - self._count_in_progress_runs()

        # Possibly under 0 if runs were launched without queuing
        if max_runs_to_launch <= 0:
            return

        runs = self._get_queued_runs(limit=max_runs_to_launch)
        for run in runs:
            external_pipeline = self._get_external_pipeline(run)

            self._instance.launch_run(run.run_id, external_pipeline)
