from collections import namedtuple
from contextlib import contextmanager
from typing import Generator

from dagster.cli.pipeline import create_external_pipeline_run
from dagster.core.execution.build_resources import build_resources
from dagster.core.execution.execution_results import PipelineResult


class SubmitResult(
    namedtuple(
        "_SubmitResult",
        "pipeline_run instance pipeline_def mode resource_defs run_config external_execution_plan",
    )
):
    def __new__(
        cls, pipeline_run, instance, pipeline_def, mode, run_config, external_execution_plan
    ):
        return super(SubmitResult, cls).__new__(
            cls,
            pipeline_run,
            instance,
            pipeline_def,
            mode,
            pipeline_def.get_mode_definition(mode).resource_defs,
            run_config,
            external_execution_plan,
        )

    @property
    def event_logs(self):
        return self.instance.all_logs(self.pipeline_run.run_id)

    @property
    def is_run_complete(self) -> bool:
        event_logs = self.event_logs
        return any(
            event.dagster_event.is_pipeline_success or event.dagster_event.is_pipeline_failure
            for event in event_logs
            if event.dagster_event
        )

    @property
    def all_events(self):
        return [event.dagster_event for event in self.event_logs if event.dagster_event]

    @contextmanager
    def wait_for_result(self) -> Generator[PipelineResult, None, None]:
        while not self.is_run_complete:
            continue
        resource_configs = self.run_config.get("resources") if self.run_config else None
        with build_resources(
            self.resource_defs, self.instance, resource_configs, self.pipeline_run
        ) as resource_context:
            yield PipelineResult(
                self.pipeline_def,
                self.all_events,
                resource_context,
                self.resource_defs,
                self.mode,
                self.run_config,
                self.pipeline_run,
                self.external_execution_plan,
            )


def submit(workspace, target, instance, mode=None, run_config=None) -> SubmitResult:
    with workspace.find_external_target(target) as external_targets:
        pipeline_def, external_pipeline, external_repo, repo_location = external_targets
        pipeline_run, external_execution_plan = create_external_pipeline_run(
            instance=instance,
            repo_location=repo_location,
            external_repo=external_repo,
            external_pipeline=external_pipeline,
            run_config=run_config,
            mode=mode,
            preset=None,
            tags=None,
            solid_selection=None,
            run_id=None,
        )

        instance.submit_run(pipeline_run.run_id, external_pipeline)
        return SubmitResult(
            pipeline_run, instance, pipeline_def, mode, run_config, external_execution_plan
        )
