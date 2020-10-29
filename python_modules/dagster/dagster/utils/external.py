import contextlib

from dagster import check
from dagster.core.host_representation import (
    ExternalPipeline,
    RepositoryLocation,
    RepositoryLocationHandle,
)
from dagster.core.host_representation.selector import PipelineSelector
from dagster.core.origin import PipelineOrigin
from dagster.core.storage.pipeline_run import PipelineRun


@contextlib.contextmanager
def external_pipeline_from_run(instance, pipeline_run):
    check.inst_param(pipeline_run, "pipeline_run", PipelineRun)
    pipeline_origin = check.inst(pipeline_run.pipeline_origin, PipelineOrigin)

    with RepositoryLocationHandle.create_from_repository_origin(
        pipeline_origin.repository_origin, instance
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
        yield external_pipeline
