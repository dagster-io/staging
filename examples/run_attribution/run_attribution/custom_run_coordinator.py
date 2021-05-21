import warnings

from dagster.core.host_representation import ExternalPipeline
from dagster.core.run_coordinator import QueuedRunCoordinator
from dagster.core.storage.pipeline_run import PipelineRun
from flask import has_request_context, request


class CustomRunCoordinator(QueuedRunCoordinator):
    def get_email(self, session_token: str) -> str:
        return session_token

    def submit_run(
        self, pipeline_run: PipelineRun, external_pipeline: ExternalPipeline
    ) -> PipelineRun:
        session_token = (
            request.headers.get("Dagster-Session-Token", None) if has_request_context() else None
        )
        email = self.get_email(session_token)
        if email:
            self._instance.add_run_tags(pipeline_run.run_id, {"user": email})
        else:
            warnings.warn(f"Couldn't decode session token {email}")
        return super().submit_run(pipeline_run, external_pipeline)
